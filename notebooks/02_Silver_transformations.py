# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_Silver_Transformation — Bronze -> `silver_seismic_events` (SCD Type 1)
# MAGIC
# MAGIC Reads Bronze data for a specific `batch_id`, parses GeoJSON properties with strict explicit types,
# MAGIC routes bad/corrupted records to quarantine, logs schema drift, and performs an idempotent MERGE INTO.

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

dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("force", "false", ["false", "true"])
dbutils.widgets.text("database", "seismo")

DB = dbutils.widgets.get("database")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"USE SCHEMA {DB}")

BRONZE = "bronze_seismic_events"
SILVER = "silver_seismic_events"
QUARANTINE = "silver_seismic_events_quarantine"
SILVER_ERR = "error_log"
EXEC_LOG = "pipeline_execution_log"
LAYER = "BRONZE_TO_SILVER"

# COMMAND ----------

# MAGIC %md ## DDL Statements (Idempotent Table Creation)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
  event_id                STRING    COMMENT 'USGS event id (Primary Key)',
  magnitude               DOUBLE,
  magnitude_type          STRING,
  place                   STRING,
  longitude               DOUBLE,
  latitude                DOUBLE,
  depth_km                DOUBLE,
  event_time              TIMESTAMP,
  event_updated_time      TIMESTAMP,
  status                  STRING,
  felt_reports            INT,
  cdi                     DOUBLE,
  mmi                     DOUBLE,
  alert_level             STRING,
  significance            INT,
  tsunami_flag            BOOLEAN,
  network_code            STRING,
  event_type              STRING,
  is_active               BOOLEAN   COMMENT 'FALSE if status == deleted',
  country                 STRING,
  region                  STRING,
  depth_category          STRING,
  magnitude_category      STRING,
  source_batch_id         STRING,
  modification_sequence   INT       COMMENT 'increments each time this event is updated',
  is_most_recent_valid    BOOLEAN   COMMENT 'TRUE for the latest valid version',
  created_timestamp       TIMESTAMP COMMENT 'when first created',
  updated_timestamp       TIMESTAMP COMMENT 'when last modified',
  year                    INT,
  month                   INT
) USING DELTA
PARTITIONED BY (year, month)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE} (
  quarantine_id       BIGINT GENERATED ALWAYS AS IDENTITY,
  event_id            STRING,
  batch_id            STRING,
  quarantine_reason   STRING,
  failed_columns      STRING,
  raw_json            STRING,
  error_details       STRING,
  quarantine_timestamp TIMESTAMP,
  reprocessing_status STRING,
  created_timestamp   TIMESTAMP,
  updated_timestamp   TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_ERR} (
  error_id            BIGINT GENERATED ALWAYS AS IDENTITY,
  error_timestamp     TIMESTAMP,
  layer               STRING,
  notebook_name       STRING,
  batch_id            STRING,
  error_type          STRING,
  error_message       STRING,
  failed_record       STRING,
  stack_trace         STRING,
  resolution_status   STRING,
  resolved_by         STRING,
  resolution_notes    STRING,
  created_timestamp   TIMESTAMP,
  updated_timestamp   TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

def utc_now():
    return datetime.now(timezone.utc)

def append_with_identity(df, table):
    cols = ", ".join(df.columns)
    df.createOrReplaceTempView("_tmp_append")
    spark.sql(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _tmp_append")

def already_succeeded(batch_id):
    return (spark.table(EXEC_LOG)
            .filter((F.col("batch_id") == batch_id) & (F.col("layer") == LAYER)
                    & F.col("status").isin("SUCCESS", "PARTIAL"))
            .limit(1).count() > 0)

def write_exec_log(start, end, batch_id, source_param, status, m, err_msg):
    row = (start, end, "02_Silver_transformations", batch_id, LAYER, "BATCH", source_param, status,
           int(m["total"]), int(m["inserted"]), int(m["updated"]), int(m["errored"]),
           err_msg, int((end - start).total_seconds()), utc_now())
    append_with_identity(spark.createDataFrame([row], EXECUTION_LOG_SCHEMA), EXEC_LOG)

# COMMAND ----------

# MAGIC %md ## Core Transformation: `run_silver`

# COMMAND ----------

def run_silver(batch_id, force=False):
    start = utc_now()
    load_ts = start
    m = {"total": 0, "inserted": 0, "updated": 0, "errored": 0}
    status, err_msg, failure = "SUCCESS", None, None

    if not batch_id:
        raise ValueError("batch_id widget parameter is required.")

    if not force and already_succeeded(batch_id):
        write_exec_log(start, utc_now(), batch_id, batch_id, "SKIPPED", m,
                       "batch_id already succeeded in Silver; pass force=true to re-run")
        print(f"SKIPPED: Silver for {batch_id} already succeeded.")
        return m

    try:
        # 1) Read raw records for this batch from Bronze
        bronze_df = spark.table(BRONZE).filter(F.col("batch_id") == batch_id)
        m["total"] = bronze_df.count()

        # Log Bronze read operation
        log_file_operation(
            layer=LAYER_SILVER,
            operation_type=OP_READ,
            file_name=BRONZE,
            batch_id=batch_id,
            records_affected=m["total"],
            status=STATUS_SUCCESS,
            file_path=f"table://{BRONZE}"
        )

        if m["total"] == 0:
            write_exec_log(start, utc_now(), batch_id, batch_id, "SUCCESS", m, "No records found in Bronze for batch_id")
            print(f"SUCCESS: 0 records found in Bronze for batch_id={batch_id}")
            return m

        # 2) Parse JSON structure explicitly using FEATURE_SCHEMA (No inferSchema)
        parsed = bronze_df.withColumn("parsed", F.from_json("raw_json", FEATURE_SCHEMA))

        # 3) Check Schema Drift (Detect unexpected keys in raw_json)
        top_keys = parsed.select(F.explode(F.json_object_keys("raw_json")).alias("k")).distinct().collect()
        unexpected_keys = [r["k"] for r in top_keys if r["k"] not in EXPECTED_TOP_KEYS]
        if unexpected_keys:
            for key in unexpected_keys:
                err_row = (load_ts, LAYER, "02_Silver_transformations", batch_id, "SCHEMA_DRIFT", 
                          f"Unexpected top-level key found: {key}", None, None, "UNRESOLVED", None, None, 
                          load_ts, None)
                append_with_identity(spark.createDataFrame([err_row], ERROR_LOG_SCHEMA), SILVER_ERR)

        # 4) Extract and Cast fields
        extracted = parsed.select(
            "raw_json",
            "batch_id",
            F.col("parsed.id").alias("event_id"),
            F.col("parsed.properties.mag").cast("double").alias("magnitude"),
            F.col("parsed.properties.magType").alias("magnitude_type"),
            F.col("parsed.properties.place").alias("place"),
            F.col("parsed.geometry.coordinates").getItem(0).cast("double").alias("longitude"),
            F.col("parsed.geometry.coordinates").getItem(1).cast("double").alias("latitude"),
            F.col("parsed.geometry.coordinates").getItem(2).cast("double").alias("depth_km"),
            (F.col("parsed.properties.time") / 1000).cast("timestamp").alias("event_time"),
            (F.col("parsed.properties.updated") / 1000).cast("timestamp").alias("event_updated_time"),
            F.col("parsed.properties.status").alias("status"),
            F.col("parsed.properties.felt").cast("int").alias("felt_reports"),
            F.col("parsed.properties.cdi").cast("double").alias("cdi"),
            F.col("parsed.properties.mmi").cast("double").alias("mmi"),
            F.col("parsed.properties.alert").alias("alert_level"),
            F.col("parsed.properties.sig").cast("int").alias("significance"),
            (F.col("parsed.properties.tsunami") == 1).alias("tsunami_flag"),
            F.col("parsed.properties.net").alias("network_code"),
            F.col("parsed.properties.type").alias("event_type")
        )

        # 5) Validation / Quarantine Logic
        invalid_cond = (
            F.col("event_id").isNull() |
            F.col("event_time").isNull() |
            F.col("event_updated_time").isNull() |
            (F.col("latitude") < -90) | (F.col("latitude") > 90) |
            (F.col("longitude") < -180) | (F.col("longitude") > 180) |
            (F.col("magnitude") > 10)
        )

        bad_rows = extracted.filter(invalid_cond)
        good_rows = extracted.filter(~invalid_cond)

        m["errored"] = bad_rows.count()
        if m["errored"] > 0:
            quarantine_records = bad_rows.select(
                F.col("event_id"),
                F.col("batch_id"),
                F.lit("Validation bounds or required null check failed").alias("quarantine_reason"),
                F.lit("event_id/time/lat/lon/mag").alias("failed_columns"),
                F.col("raw_json"),
                F.lit("See validation conditions in transformation logic").alias("error_details"),
                F.lit(load_ts).alias("quarantine_timestamp"),
                F.lit("PENDING").alias("reprocessing_status"),
                F.lit(load_ts).alias("created_timestamp"),
                F.lit(None).cast("timestamp").alias("updated_timestamp"),
            )
            append_with_identity(quarantine_records, QUARANTINE)
            
            # Log quarantine operation
            log_file_operation(
                layer=LAYER_SILVER,
                operation_type=OP_CREATE,
                file_name=QUARANTINE,
                batch_id=batch_id,
                records_affected=m["errored"],
                status=STATUS_SUCCESS,
                file_path=f"table://{QUARANTINE}"
            )

        # 6) Enrich Valid Records
        enriched = good_rows.withColumn(
            "is_active", F.col("status") != "deleted"
        ).withColumn(
            "country", F.element_at(F.split(F.col("place"), ", "), -1)
        ).withColumn(
            "region", F.when(F.col("place").contains(","), F.element_at(F.split(F.col("place"), ", "), 1)).otherwise(F.col("place"))
        ).withColumn(
            "depth_category",
            F.when(F.col("depth_km") < 70, "Shallow")
             .when(F.col("depth_km") < 300, "Intermediate")
             .otherwise("Deep")
        ).withColumn(
            "magnitude_category",
            F.when(F.col("magnitude") < 3.0, "Minor")
             .when(F.col("magnitude") < 4.0, "Light")
             .when(F.col("magnitude") < 5.0, "Moderate")
             .when(F.col("magnitude") < 6.0, "Strong")
             .when(F.col("magnitude") < 7.0, "Major")
             .otherwise("Great")
        ).withColumn(
            "source_batch_id", F.col("batch_id")
        ).withColumn(
            "modification_sequence", F.lit(1)
        ).withColumn(
            "is_most_recent_valid", F.lit(True)
        ).withColumn(
            "created_timestamp", F.lit(load_ts)
        ).withColumn(
            "updated_timestamp", F.lit(load_ts)
        ).withColumn(
            "year", F.year("event_time")
        ).withColumn(
            "month", F.month("event_time")
        ).drop("raw_json", "batch_id")

        # 7) Source De-duplication per event_id (keep highest event_updated_time)
        w = Window.partitionBy("event_id").orderBy(F.col("event_updated_time").desc())
        deduped = enriched.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        # 8) Idempotent MERGE INTO
        staged_count = deduped.count()
        if staged_count > 0:
            target_table = DeltaTable.forName(spark, SILVER)
            
            # Execute Upsert
            (target_table.alias("t")
             .merge(deduped.alias("s"), "t.event_id = s.event_id")
             .whenMatchedUpdate(
                 condition="s.event_updated_time > t.event_updated_time",
                 set={
                     "magnitude": "s.magnitude",
                     "magnitude_type": "s.magnitude_type",
                     "place": "s.place",
                     "longitude": "s.longitude",
                     "latitude": "s.latitude",
                     "depth_km": "s.depth_km",
                     "event_time": "s.event_time",
                     "event_updated_time": "s.event_updated_time",
                     "status": "s.status",
                     "felt_reports": "s.felt_reports",
                     "cdi": "s.cdi",
                     "mmi": "s.mmi",
                     "alert_level": "s.alert_level",
                     "significance": "s.significance",
                     "tsunami_flag": "s.tsunami_flag",
                     "network_code": "s.network_code",
                     "event_type": "s.event_type",
                     "is_active": "s.is_active",
                     "country": "s.country",
                     "region": "s.region",
                     "depth_category": "s.depth_category",
                     "magnitude_category": "s.magnitude_category",
                     "source_batch_id": "s.source_batch_id",
                     "modification_sequence": "t.modification_sequence + 1",
                     "is_most_recent_valid": "true",
                     "updated_timestamp": "s.updated_timestamp",
                     "year": "s.year",
                     "month": "s.month"
                 }
             )
             .whenNotMatchedInsertAll()
             .execute())

            m["inserted"] = staged_count
            
            # Log successful Silver write operation
            log_file_operation(
                layer=LAYER_SILVER,
                operation_type=OP_UPDATE,
                file_name=SILVER,
                batch_id=batch_id,
                records_affected=staged_count,
                status=STATUS_SUCCESS,
                file_path=f"table://{SILVER}"
            )

        if m["errored"] > 0:
            status = "PARTIAL"
            err_msg = f"{m['errored']} record(s) sent to quarantine table {QUARANTINE}"

    except Exception as e:
        status, err_msg, failure = "FAILED", str(e)[:2000], e

    write_exec_log(start, utc_now(), batch_id, batch_id, status, m, err_msg)
    print(f"[{status}] Silver {batch_id}: total={m['total']} inserted={m['inserted']} errored={m['errored']}")
    if failure is not None:
        raise failure
    return m

# COMMAND ----------

# MAGIC %md ## Execution

# COMMAND ----------

batch_id_param = dbutils.widgets.get("batch_id")
force_param = dbutils.widgets.get("force") == "true"

run_silver(batch_id_param, force_param)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Proof 1: Verify Execution Log
# MAGIC SELECT execution_id, batch_id, layer, status, records_processed, records_inserted, execution_duration_sec
# MAGIC FROM pipeline_execution_log
# MAGIC ORDER BY execution_id DESC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Proof 2: Verify Idempotency (0 rows returned)
# MAGIC SELECT event_id, COUNT(*) AS dup_count 
# MAGIC FROM silver_seismic_events 
# MAGIC GROUP BY event_id 
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Proof 3: Preview Processed Silver Data
# MAGIC SELECT event_id, magnitude, place, latitude, longitude, depth_km, region, load_timestamp
# MAGIC FROM silver_seismic_events 
# MAGIC LIMIT 10;