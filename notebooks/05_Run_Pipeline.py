# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05_Run_Pipeline — End-to-end orchestrator (Raw → Bronze → Silver → Gold → Optimize)
# MAGIC
# MAGIC One entry point for the whole Medallion pipeline. The Databricks Job in `jobs/seismo_pipeline_job.yml`
# MAGIC runs this notebook on a **file-arrival trigger** on the raw Volume, so every new incremental GeoJSON file
# MAGIC flows through Bronze and Silver and **automatically refreshes the Gold star schema**.
# MAGIC
# MAGIC **Parameters (widgets)**
# MAGIC | widget | meaning |
# MAGIC |---|---|
# MAGIC | `ingestion_type` | `INCREMENTAL_LOAD` (default) / `FULL_LOAD` / `BACKFILL` |
# MAGIC | `source_path` | file or folder to ingest. Blank + INCREMENTAL → newest `usgs_earthquake_incremental_*.json` in the raw Volume |
# MAGIC | `batch_id` | blank → derived from the file name timestamp (`INCR_20261021_120000`) or `{PREFIX}_{today}` |
# MAGIC | `force` | `true` = re-run Bronze/Silver for a batch_id that already succeeded |
# MAGIC | `gold_full_refresh` | `true` = rebuild Gold from all of Silver (ignores the Gold watermark) |
# MAGIC | `run_optimize` | `true` = run `04_Gold_Optimize` after Gold |
# MAGIC
# MAGIC Every child notebook logs its own steps; this notebook adds one `END_TO_END` row to
# MAGIC `pipeline_execution_log` with the total wall-clock time (used for the FinOps estimate).

# COMMAND ----------

# MAGIC %run ./config/schemas

# COMMAND ----------

# MAGIC %run ./config/constants

# COMMAND ----------

# MAGIC %run ./config/utils

# COMMAND ----------

import json
import re

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.dropdown("ingestion_type", "INCREMENTAL_LOAD", ["INCREMENTAL_LOAD", "FULL_LOAD", "BACKFILL"])
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("force", "false", ["false", "true"])
dbutils.widgets.dropdown("gold_full_refresh", "false", ["false", "true"])
dbutils.widgets.dropdown("run_optimize", "true", ["true", "false"])
dbutils.widgets.text("database", "seismo")
dbutils.widgets.text("raw_path", SOURCE_BASE_PATH)

DB = dbutils.widgets.get("database")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"USE SCHEMA {DB}")

EXEC_LOG = "pipeline_execution_log"
TIMEOUT_SEC = 3600

# COMMAND ----------

# MAGIC %md ## Resolve source file and batch_id

# COMMAND ----------

ingestion_type = dbutils.widgets.get("ingestion_type")
source_path = dbutils.widgets.get("source_path").strip()
batch_id = dbutils.widgets.get("batch_id").strip()
raw_path = dbutils.widgets.get("raw_path").rstrip("/")

if not source_path:
    if ingestion_type == LOAD_TYPE_INCREMENTAL:
        incr = [f for f in dbutils.fs.ls(raw_path)
                if re.match(r"usgs_earthquake_incremental.*\.json$", f.name)]
        if not incr:
            raise FileNotFoundError(f"No usgs_earthquake_incremental*.json found in {raw_path}")
        source_path = max(incr, key=lambda f: f.modificationTime).path
    elif ingestion_type == LOAD_TYPE_FULL:
        source_path = SOURCE_FULL_LOAD
    else:
        source_path = raw_path
source_path = source_path.replace("dbfs:", "")

if not batch_id:
    ts = re.search(r"(\d{8}_\d{6})", source_path)
    if ingestion_type == LOAD_TYPE_INCREMENTAL and ts:
        batch_id = f"{PREFIX_INCR}_{ts.group(1)}"   # unique per file -> safe for hourly runs
    else:
        batch_id = make_batch_id(ingestion_type, utc_now().strftime("%Y-%m-%d"))

force = dbutils.widgets.get("force")
print(f"ingestion_type={ingestion_type}\nsource_path={source_path}\nbatch_id={batch_id}\nforce={force}")

# COMMAND ----------

# MAGIC %md ## Run the stages in order (a failure stops the pipeline and is logged)

# COMMAND ----------

def write_exec_log(start, end, status, m, err_msg=None):
    row = (start, end, NOTEBOOK_PIPELINE, batch_id, "END_TO_END", ingestion_type, source_path, status,
           int(m.get("total", 0)), int(m.get("inserted", 0)), int(m.get("updated", 0)), 0,
           err_msg[:2000] if err_msg else None, int((end - start).total_seconds()), utc_now())
    append_with_identity(spark.createDataFrame([row], EXECUTION_LOG_SCHEMA), EXEC_LOG)

stages = [
    ("01_Bronze_Ingestion", {"source_path": source_path, "ingestion_type": ingestion_type,
                             "batch_id": batch_id, "force": force, "database": DB}),
    ("02_Silver_transformations", {"batch_id": batch_id, "force": force, "database": DB}),
    ("03_Gold_Transformations", {"batch_id": batch_id, "database": DB,
                                 "full_refresh": dbutils.widgets.get("gold_full_refresh")}),
]
if dbutils.widgets.get("run_optimize") == "true":
    stages.append(("04_Gold_Optimize", {"batch_id": batch_id, "database": DB}))

pipeline_start = utc_now()
timings = []
current = None
try:
    for nb, args in stages:
        current = nb
        t0 = utc_now()
        print(f"▶ {nb} ...")
        out = dbutils.notebook.run(f"./{nb}", TIMEOUT_SEC, args)
        secs = round((utc_now() - t0).total_seconds(), 1)
        timings.append((nb, secs))
        print(f"  ✓ {nb} finished in {secs}s {('-> ' + out[:300]) if out else ''}")
except Exception as e:
    write_exec_log(pipeline_start, utc_now(), STATUS_FAILED, {}, f"{current} failed: {e}")
    raise

gold_rows = spark.table(GOLD_FACT_SEISMIC_EVENT).count()
write_exec_log(pipeline_start, utc_now(), STATUS_SUCCESS, {"total": gold_rows},
               "; ".join(f"{nb}={s}s" for nb, s in timings))

# COMMAND ----------

display(spark.createDataFrame(timings, "stage STRING, seconds DOUBLE"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest run, every layer: what was processed and how long it took
# MAGIC SELECT execution_id, layer, notebook_name, source_param, status,
# MAGIC        records_processed, records_inserted, records_updated, records_errored, execution_duration_sec
# MAGIC FROM pipeline_execution_log
# MAGIC WHERE batch_id = (SELECT batch_id FROM pipeline_execution_log WHERE layer = 'END_TO_END'
# MAGIC                   ORDER BY execution_id DESC LIMIT 1)
# MAGIC ORDER BY execution_id;

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "stages": timings}))