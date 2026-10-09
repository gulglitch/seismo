# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01_Bronze_Ingestion — Raw GeoJSON → `bronze_seismic_events` (SCD Type 2)
# MAGIC
# MAGIC **Parameters (widgets)**
# MAGIC | widget | meaning |
# MAGIC |---|---|
# MAGIC | `source_path` | a file **or folder** of GeoJSON (folder = backfill over many files) |
# MAGIC | `ingestion_type` | `FULL_LOAD` / `INCREMENTAL_LOAD` / `BACKFILL` |
# MAGIC | `batch_id` | optional; blank → `{FULL\|INCR\|BACKFILL}_{run_date}` |
# MAGIC | `run_date` | optional `YYYY-MM-DD`; blank → today (UTC). Used in the default batch_id |
# MAGIC | `force` | `true` = re-run a batch_id that already SUCCEEDED |
# MAGIC | `database` | schema/database holding all tables |
# MAGIC
# MAGIC **Guarantees:** no schema inference · one `load_timestamp` per batch · SCD2 history via a single MERGE ·
# MAGIC re-running the same data inserts/updates **0** rows · every run (success or failure) is written to `pipeline_execution_log`.

# COMMAND ----------

# MAGIC %run ./config/schemas

# COMMAND ----------

# MAGIC %run ./config/constants

# COMMAND ----------

# MAGIC %run ./config/utils

# COMMAND ----------

import traceback
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("source_path", "/Volumes/workspace/seismo/raw/usgs_earthquake_full_load_2019_2024.json")
dbutils.widgets.dropdown("ingestion_type", "FULL_LOAD", ["FULL_LOAD", "INCREMENTAL_LOAD", "BACKFILL"])
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.dropdown("force", "false", ["false", "true"])
dbutils.widgets.text("database", "seismo")

DB = dbutils.widgets.get("database")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"USE SCHEMA {DB}")

BRONZE = "bronze_seismic_events"
BRONZE_ERR = "error_log"  # unified error log table (shared across all layers)
EXEC_LOG = "pipeline_execution_log"
LAYER = "RAW_TO_BRONZE"

# COMMAND ----------

# MAGIC %md ## Tables (idempotent DDL — safe to run every time)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
  event_id            STRING     COMMENT 'USGS event id (business key)',
  raw_json            STRING     COMMENT 'complete GeoJSON feature, unmodified',
  source_file_name    STRING,
  batch_id            STRING,
  ingestion_type      STRING,
  is_current          BOOLEAN    COMMENT 'TRUE = latest version of this event_id',
  valid_from          TIMESTAMP  COMMENT 'batch-level ingest time of the version that created this row',
  valid_to            TIMESTAMP  COMMENT 'valid_from of the batch that superseded it; NULL if current',
  record_hash         STRING     COMMENT 'md5(raw_json) — change detection',
  created_timestamp   TIMESTAMP  COMMENT 'when this row was first created',
  updated_timestamp   TIMESTAMP  COMMENT 'when this row was last modified',
  year                INT,
  month               INT
) USING DELTA
PARTITIONED BY (ingestion_type, year, month)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EXEC_LOG} (
  execution_id           BIGINT GENERATED ALWAYS AS IDENTITY,
  execution_start        TIMESTAMP,
  execution_end          TIMESTAMP,
  notebook_name          STRING,
  batch_id               STRING,
  layer                  STRING,
  load_type              STRING,
  source_param           STRING,
  status                 STRING,
  records_processed      BIGINT,
  records_inserted       BIGINT,
  records_updated        BIGINT,
  records_errored        BIGINT,
  error_message          STRING,
  execution_duration_sec INT,
  created_timestamp      TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

PREFIX = {"FULL_LOAD": "FULL", "INCREMENTAL_LOAD": "INCR", "BACKFILL": "BACKFILL"}


def utc_now():
    return datetime.now(timezone.utc)          # tz-aware → no driver-timezone surprises


def append_with_identity(df, table):
    """INSERT with an explicit column list so GENERATED ALWAYS identity columns are skipped."""
    cols = ", ".join(df.columns)
    df.createOrReplaceTempView("_tmp_append")
    spark.sql(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _tmp_append")


def make_batch_id(ingestion_type, run_date):
    # Default convention: FULL_2026-10-03 / INCR_2026-10-04 / BACKFILL_2026-09-01.
    # Need a 2nd run of the same type on the same day? Pass your own batch_id (e.g. INCR_2026-10-04_2).
    return f"{PREFIX[ingestion_type]}_{run_date}"


def already_succeeded(batch_id):
    return (spark.table(EXEC_LOG)
            .filter((F.col("batch_id") == batch_id) & (F.col("layer") == LAYER)
                    & F.col("status").isin("SUCCESS", "PARTIAL"))
            .limit(1).count() > 0)


def write_exec_log(start, end, batch_id, ingestion_type, source_param, status, m, err_msg):
    row = (start, end, "01_Bronze_Ingestion", batch_id, LAYER, ingestion_type, source_param, status,
           int(m["total"]), int(m["inserted"]), int(m["updated"]), int(m["errored"]),
           err_msg, int((end - start).total_seconds()), utc_now())
    append_with_identity(spark.createDataFrame([row], EXECUTION_LOG_SCHEMA), EXEC_LOG)


def classify_error(e):
    msg = str(e)
    if "PATH_NOT_FOUND" in msg or "Path does not exist" in msg or "FileNotFound" in msg:
        return "FILE_NOT_FOUND"
    if "MALFORMED" in msg.upper() or "JsonParseException" in msg or "Malformed records" in msg:
        return "MALFORMED_JSON"
    return "UNEXPECTED_ERROR"


def log_bronze_failure(ts, batch_id, source_path, e):
    row = (ts, LAYER, "01_Bronze_Ingestion", batch_id, classify_error(e), str(e)[:2000], None, 
           traceback.format_exc()[:8000], "UNRESOLVED", None, None, utc_now(), None)
    append_with_identity(spark.createDataFrame([row], ERROR_LOG_SCHEMA), BRONZE_ERR)

# COMMAND ----------

# MAGIC %md ## Core: `run_bronze`
# MAGIC SCD2 in **one MERGE**. For every changed event we put two copies in the MERGE source:
# MAGIC * copy A, `merge_key = event_id` → matches the current row → *expires it* (`is_current=false`, `valid_to=load_ts`)
# MAGIC * copy B, `merge_key = NULL` → never matches → *inserts the new version*
# MAGIC
# MAGIC Unchanged events (same `record_hash` as the current row) are filtered out first — that is what makes re-runs a no-op.

# COMMAND ----------

def run_bronze(source_path, batch_id, ingestion_type, force=False):
    start = utc_now()
    load_ts = start                                   # Decision 5, option C: captured ONCE, reused for the whole batch
    m = {"total": 0, "inserted": 0, "updated": 0, "errored": 0}
    status, err_msg, failure = "SUCCESS", None, None

    if not force and already_succeeded(batch_id):
        write_exec_log(start, utc_now(), batch_id, ingestion_type, source_path, "SKIPPED", m,
                       "batch_id already succeeded; pass force=true to re-run")
        print(f"SKIPPED: {batch_id} already succeeded (use force=true to re-run)")
        return m

    try:
        # 1) READ with an explicit schema (NO inference). FAILFAST → malformed JSON raises → logged below.
        raw_df = (spark.read
                  .schema(ENVELOPE_SCHEMA)
                  .option("multiline", "true")
                  .option("mode", "FAILFAST")
                  .json(source_path))
        
        # Log successful file read
        log_file_operation(
            layer=LAYER_BRONZE,
            operation_type=OP_READ,
            file_name=source_path.split("/")[-1],
            batch_id=batch_id,
            records_affected=0,  # Will be updated after processing
            status=STATUS_SUCCESS,
            file_path=source_path
        )

        # 2) One row per feature; raw_json = untouched feature text; source_file_name works for file OR folder paths
        feats = raw_df.select(
            F.col("_metadata.file_name").alias("source_file_name"),
            F.explode_outer("features").alias("raw_json"),
        )
        feats = (feats
                 .withColumn("event_id", F.get_json_object("raw_json", "$.id"))
                 .withColumn("updated_ms", F.get_json_object("raw_json", "$.properties.updated").cast("long")))

        # 3) Records with no event_id → bronze_error_log (MISSING_KEY), excluded from the load
        bad = feats.filter(F.col("event_id").isNull())
        m["errored"] = bad.count()
        if m["errored"] > 0:
            append_with_identity(
                bad.select(
                    F.lit(load_ts).alias("error_timestamp"),
                    F.lit(LAYER).alias("layer"),
                    F.lit("01_Bronze_Ingestion").alias("notebook_name"),
                    F.lit(batch_id).alias("batch_id"),
                    F.lit("MISSING_KEY").alias("error_type"),
                    F.lit("Feature has no id").alias("error_message"),
                    F.substring("raw_json", 1, 4000).alias("failed_record"),
                    F.lit(None).cast("string").alias("stack_trace"),
                    F.lit("UNRESOLVED").alias("resolution_status"),
                    F.lit(None).cast("string").alias("resolved_by"),
                    F.lit(None).cast("string").alias("resolution_notes"),
                    F.lit(load_ts).alias("created_timestamp"),
                    F.lit(None).cast("timestamp").alias("updated_timestamp"),
                ), BRONZE_ERR)

        # 4) De-dupe inside the batch (MERGE needs ≤1 source row per key): keep the most recently updated
        w = Window.partitionBy("event_id").orderBy(F.col("updated_ms").desc_nulls_last())
        good = (feats.filter(F.col("event_id").isNotNull())
                .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
                .drop("_rn", "updated_ms"))

        # 5) Add Bronze metadata. Every literal uses the single load_ts → identical across the batch.
        staged = good.select(
            "event_id", "raw_json", "source_file_name",
            F.lit(batch_id).alias("batch_id"),
            F.lit(ingestion_type).alias("ingestion_type"),
            F.lit(True).alias("is_current"),
            F.lit(load_ts).alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.md5("raw_json").alias("record_hash"),
            F.lit(load_ts).alias("created_timestamp"),
            F.lit(load_ts).alias("updated_timestamp"),
            F.lit(load_ts.year).alias("year"),
            F.lit(load_ts.month).alias("month"),
        )

        # 6) Which events actually need a new version? (new event_id OR different hash vs current row)
        current = (spark.table(BRONZE).filter("is_current = true")
                   .select("event_id", F.col("record_hash").alias("cur_hash")))
        changed = (staged.join(current, "event_id", "left")
                   .filter(F.col("cur_hash").isNull() | (F.col("cur_hash") != F.col("record_hash")))
                   .withColumn("had_current", F.col("cur_hash").isNotNull())
                   .drop("cur_hash"))

        # metrics BEFORE the merge (table is still in its pre-merge state)
        m["total"] = staged.count()
        agg = changed.agg(F.count("*").alias("n"), F.sum(F.col("had_current").cast("int")).alias("v")).first()
        n_changed, n_versioned = int(agg["n"] or 0), int(agg["v"] or 0)
        m["inserted"] = n_changed          # new rows written (new events + new versions)
        m["updated"] = n_versioned         # old rows expired (is_current → false)

        # 7) SCD2 MERGE
        if n_changed > 0:
            expire = changed.filter("had_current").withColumn("merge_key", F.col("event_id"))
            insert = changed.withColumn("merge_key", F.lit(None).cast("string"))
            src = expire.unionByName(insert).drop("had_current")

            src_cols = set(src.columns)
            tgt_cols = [f.name for f in spark.table(BRONZE).schema.fields if f.name in src_cols]
            (DeltaTable.forName(spark, BRONZE).alias("t")
             .merge(src.alias("s"), "t.event_id = s.merge_key AND t.is_current = true")
             .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s.valid_from"})
             .whenNotMatchedInsert(condition="s.merge_key IS NULL",
                                   values={c: f"s.{c}" for c in tgt_cols})
             .execute())
            
            # Log successful file operation
            log_file_operation(
                layer=LAYER_BRONZE,
                operation_type=OP_UPDATE,
                file_name=BRONZE,
                batch_id=batch_id,
                records_affected=n_changed,
                status=STATUS_SUCCESS,
                file_path=source_path
            )

        if m["errored"] > 0:
            status = "PARTIAL"
            err_msg = f"{m['errored']} feature(s) without event_id logged to {BRONZE_ERR}"

    except Exception as e:
        status, err_msg, failure = "FAILED", str(e)[:2000], e
        log_bronze_failure(utc_now(), batch_id, source_path, e)

    write_exec_log(start, utc_now(), batch_id, ingestion_type, source_path, status, m, err_msg)
    print(f"[{status}] {batch_id}: processed={m['total']} inserted={m['inserted']} "
          f"updated(expired)={m['updated']} errored={m['errored']}")
    if failure is not None:
        raise failure                    # job/notebook shows red AFTER the failure was logged
    return m

# COMMAND ----------

# MAGIC %md ## Run (reads the widgets)

# COMMAND ----------

# DBTITLE 1,Run
source_path    = dbutils.widgets.get("source_path")
if "seismo_data" in source_path:
    source_path = SOURCE_FULL_LOAD
ingestion_type = dbutils.widgets.get("ingestion_type")
run_date       = dbutils.widgets.get("run_date") or utc_now().strftime("%Y-%m-%d")
batch_id       = dbutils.widgets.get("batch_id") or make_batch_id(ingestion_type, run_date)
force          = dbutils.widgets.get("force") == "true"

print(f"batch_id={batch_id} type={ingestion_type} path={source_path} force={force}")
metrics = run_bronze(source_path, batch_id, ingestion_type, force)

# COMMAND ----------

# MAGIC %md ## Proof cells (screenshot these for the submission)

# COMMAND ----------

# 1) Execution log — run the notebook twice with the SAME data and show the 2nd run: inserted=0, updated=0
display(spark.sql(f"""
SELECT execution_id, batch_id, layer, load_type, source_param, status,
       records_processed, records_inserted, records_updated, records_errored, execution_duration_sec
FROM {EXEC_LOG} ORDER BY execution_id DESC LIMIT 10"""))

# COMMAND ----------

# 2) Invariant: exactly ONE current row per event_id  → must return 0 rows
display(spark.sql(f"""
SELECT event_id, COUNT(*) AS n_current FROM {BRONZE}
WHERE is_current = true GROUP BY event_id HAVING COUNT(*) > 1"""))

# COMMAND ----------

# 3) Invariant: no duplicate (event_id, record_hash) among CURRENT rows, and every batch has ONE load_timestamp
display(spark.sql(f"""
SELECT batch_id, COUNT(DISTINCT valid_from) AS distinct_load_ts, COUNT(*) AS rows
FROM {BRONZE} GROUP BY batch_id ORDER BY batch_id"""))

# COMMAND ----------

# 4) History of one event (pick any event_id that changed between FULL and INCR)
display(spark.sql(f"""
SELECT event_id, get_json_object(raw_json,'$.properties.mag') AS mag,
       get_json_object(raw_json,'$.properties.status') AS status,
       batch_id, is_current, valid_from, valid_to
FROM {BRONZE}
WHERE event_id IN (SELECT event_id FROM {BRONZE} GROUP BY event_id HAVING COUNT(*) > 1 LIMIT 1)
ORDER BY valid_from"""))

# COMMAND ----------

# 5) Point-in-time ("state of the world on Oct 1") — note: NO is_current filter
display(spark.sql(f"""
SELECT event_id, get_json_object(raw_json,'$.properties.mag') AS mag, valid_from, valid_to
FROM {BRONZE}
WHERE valid_from <= TIMESTAMP '2026-10-01 23:59:59'
  AND (valid_to > TIMESTAMP '2026-10-01 23:59:59' OR valid_to IS NULL)
LIMIT 20"""))