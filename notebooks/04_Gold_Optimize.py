# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04_Gold_Optimize — Read-performance tuning for the Gold star schema
# MAGIC
# MAGIC | Technique | Applied to | Why |
# MAGIC |---|---|---|
# MAGIC | **Liquid Clustering** `CLUSTER BY (date_key, location_key)` | both fact tables | Power BI filters on date ranges and locations; clustering co-locates those rows so Delta skips files |
# MAGIC | `OPTIMIZE` | all Gold tables | Incremental MERGEs create many small files; compaction keeps file count low for fast scans |
# MAGIC | `ZORDER BY` (fallback) | facts, only if the runtime lacks liquid clustering | Same data-skipping goal on older runtimes |
# MAGIC | `ANALYZE TABLE ... COMPUTE STATISTICS` | all Gold tables | Gives the optimizer row counts so small dimensions are broadcast in star joins |
# MAGIC
# MAGIC Dimensions are tiny (< 1k rows) so they are compacted but not clustered.
# MAGIC Run after `03_Gold_Transformations` (the `05_Run_Pipeline` job does this automatically).

# COMMAND ----------

# MAGIC %run ./config/schemas

# COMMAND ----------

# MAGIC %run ./config/constants

# COMMAND ----------

# MAGIC %run ./config/utils

# COMMAND ----------

import json
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("database", "seismo")

DB = dbutils.widgets.get("database")
spark.sql(f"USE SCHEMA {DB}")

EXEC_LOG = "pipeline_execution_log"
LAYER = "GOLD_OPTIMIZE"
NOTEBOOK = "04_Gold_Optimize"
batch_id = dbutils.widgets.get("batch_id").strip() or "OPTIMIZE_" + utc_now().strftime("%Y%m%d_%H%M%S")

FACT_CLUSTER_KEYS = ["date_key", "location_key"]
FACTS = [GOLD_FACT_SEISMIC_EVENT, GOLD_FACT_DAILY_SUMMARY]

# COMMAND ----------

def detail(table):
    d = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
    return {"numFiles": d.get("numFiles"), "sizeInBytes": d.get("sizeInBytes"),
            "clusteringColumns": d.get("clusteringColumns") or []}

def write_exec_log(start, end, target, status, m, err_msg=None):
    row = (start, end, NOTEBOOK, batch_id, LAYER, "MAINTENANCE", target, status,
           int(m.get("total", 0)), 0, int(m.get("updated", 0)), 0,
           err_msg[:2000] if err_msg else None, int((end - start).total_seconds()), utc_now())
    append_with_identity(spark.createDataFrame([row], EXECUTION_LOG_SCHEMA), EXEC_LOG)

# COMMAND ----------

# MAGIC %md ## 1) Make sure facts are liquid-clustered (idempotent)

# COMMAND ----------

for t in FACTS:
    cols = detail(t)["clusteringColumns"]
    if not cols:
        try:
            spark.sql(f"ALTER TABLE {t} CLUSTER BY ({', '.join(FACT_CLUSTER_KEYS)})")
            print(f"{t}: liquid clustering enabled on {FACT_CLUSTER_KEYS}")
        except Exception as e:
            print(f"{t}: liquid clustering unavailable ({type(e).__name__}) -> will use ZORDER")
    else:
        print(f"{t}: already clustered by {cols}")

# COMMAND ----------

# MAGIC %md ## 2) OPTIMIZE + statistics, with before/after file counts

# COMMAND ----------

report = []
for t in GOLD_TABLES:
    start = utc_now()
    before = detail(t)
    try:
        if t in FACTS and not before["clusteringColumns"]:
            res = spark.sql(f"OPTIMIZE {t} ZORDER BY ({', '.join(FACT_CLUSTER_KEYS)})")
        else:
            res = spark.sql(f"OPTIMIZE {t}")
        spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
        after = detail(t)
        rows = spark.table(t).count()
        write_exec_log(start, utc_now(), t, STATUS_SUCCESS,
                       {"total": rows, "updated": max((before["numFiles"] or 0) - (after["numFiles"] or 0), 0)},
                       f"files {before['numFiles']} -> {after['numFiles']}")
        report.append((t, rows, before["numFiles"], after["numFiles"], after["sizeInBytes"],
                       ", ".join(after["clusteringColumns"]) or "-"))
    except Exception as e:
        write_exec_log(start, utc_now(), t, STATUS_FAILED, {}, str(e))
        raise

display(spark.createDataFrame(report, "table STRING, row_count LONG, files_before LONG, files_after LONG, "
                                      "size_bytes LONG, clustering_columns STRING"))

# COMMAND ----------

# MAGIC %md ## 3) Proof: data skipping on a typical BI query
# MAGIC The physical plan shows `PushedFilters` on `date_key` (predicate pushdown into the Delta scan) and a
# MAGIC `BroadcastHashJoin` for the small dimensions — no shuffle of the fact table is needed for the join.
# MAGIC After running the query, open **Spark UI → SQL / DataFrame** to see *files pruned* vs *files read*.

# COMMAND ----------

bi_query = f"""
SELECT d.year_month, l.location_label, m.magnitude_class,
       COUNT(*) AS events, ROUND(AVG(f.magnitude), 2) AS avg_mag, SUM(f.energy_joules) AS energy_j
FROM {GOLD_FACT_SEISMIC_EVENT} f
JOIN {GOLD_DIM_DATE} d            ON f.date_key = d.date_key
JOIN {GOLD_DIM_LOCATION} l        ON f.location_key = l.location_key
JOIN {GOLD_DIM_MAGNITUDE_CLASS} m ON f.magnitude_class_key = m.magnitude_class_key
WHERE f.date_key BETWEEN 20240101 AND 20241231 AND f.is_active
GROUP BY d.year_month, l.location_label, m.magnitude_class
ORDER BY events DESC
"""
print(spark.sql("EXPLAIN FORMATTED " + bi_query).first()[0][:6000])

# COMMAND ----------

display(spark.sql(bi_query))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"optimized": [r[0] for r in report]}))
