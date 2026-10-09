# Databricks notebook source
# MAGIC %md
# MAGIC # Table Row Counts - Quick Data Verification
# MAGIC 
# MAGIC **Purpose:** Quickly check how many rows are in each table
# MAGIC 
# MAGIC **Use Case:** Verify pipeline ran successfully and tables have data
# MAGIC 
# MAGIC **Last Updated:** October 9, 2026

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Tables Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_seismic_events' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM seismo.bronze_seismic_events
# MAGIC UNION ALL 
# MAGIC SELECT 'silver_seismic_events', COUNT(*) 
# MAGIC FROM seismo.silver_seismic_events
# MAGIC UNION ALL 
# MAGIC SELECT 'silver_seismic_events_quarantine', COUNT(*) 
# MAGIC FROM seismo.silver_seismic_events_quarantine
# MAGIC UNION ALL 
# MAGIC SELECT 'error_log', COUNT(*) 
# MAGIC FROM seismo.error_log
# MAGIC UNION ALL 
# MAGIC SELECT 'pipeline_execution_log', COUNT(*) 
# MAGIC FROM seismo.pipeline_execution_log
# MAGIC UNION ALL 
# MAGIC SELECT 'file_operation_log', COUNT(*) 
# MAGIC FROM seismo.file_operation_log
# MAGIC ORDER BY row_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Data Details

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: Current vs Historical records
# MAGIC SELECT 
# MAGIC   is_current,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT event_id) as unique_events
# MAGIC FROM seismo.bronze_seismic_events
# MAGIC GROUP BY is_current
# MAGIC ORDER BY is_current DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: Records by batch
# MAGIC SELECT 
# MAGIC   batch_id,
# MAGIC   ingestion_type,
# MAGIC   COUNT(*) as records,
# MAGIC   COUNT(DISTINCT event_id) as unique_events,
# MAGIC   MIN(created_timestamp) as first_loaded,
# MAGIC   MAX(created_timestamp) as last_loaded
# MAGIC FROM seismo.bronze_seismic_events
# MAGIC GROUP BY batch_id, ingestion_type
# MAGIC ORDER BY first_loaded DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Data Details

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: Records by source batch
# MAGIC SELECT 
# MAGIC   source_batch_id,
# MAGIC   COUNT(*) as records,
# MAGIC   MIN(event_time) as earliest_event,
# MAGIC   MAX(event_time) as latest_event
# MAGIC FROM seismo.silver_seismic_events
# MAGIC GROUP BY source_batch_id
# MAGIC ORDER BY source_batch_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: Check for duplicates (should return 0 rows)
# MAGIC SELECT 
# MAGIC   event_id, 
# MAGIC   COUNT(*) as duplicate_count
# MAGIC FROM seismo.silver_seismic_events
# MAGIC GROUP BY event_id
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Execution log summary
# MAGIC SELECT 
# MAGIC   batch_id,
# MAGIC   layer,
# MAGIC   load_type,
# MAGIC   status,
# MAGIC   records_processed,
# MAGIC   records_inserted,
# MAGIC   records_updated,
# MAGIC   records_errored,
# MAGIC   execution_duration_sec,
# MAGIC   execution_start
# MAGIC FROM seismo.pipeline_execution_log
# MAGIC ORDER BY execution_id DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine & Errors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarantine summary
# MAGIC SELECT 
# MAGIC   quarantine_reason,
# MAGIC   reprocessing_status,
# MAGIC   COUNT(*) as count
# MAGIC FROM seismo.silver_seismic_events_quarantine
# MAGIC GROUP BY quarantine_reason, reprocessing_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Error log summary
# MAGIC SELECT 
# MAGIC   layer,
# MAGIC   error_type,
# MAGIC   resolution_status,
# MAGIC   COUNT(*) as error_count
# MAGIC FROM seismo.error_log
# MAGIC GROUP BY layer, error_type, resolution_status
# MAGIC ORDER BY error_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary Output

# COMMAND ----------

print("="*70)
print("TABLE ROW COUNTS SUMMARY")
print("="*70)

# Get counts using Python
tables = {
    "bronze_seismic_events": spark.table("seismo.bronze_seismic_events").count(),
    "silver_seismic_events": spark.table("seismo.silver_seismic_events").count(),
    "pipeline_execution_log": spark.table("seismo.pipeline_execution_log").count(),
    "file_operation_log": spark.table("seismo.file_operation_log").count(),
    "error_log": spark.table("seismo.error_log").count(),
    "quarantine": spark.table("seismo.silver_seismic_events_quarantine").count()
}

for table, count in sorted(tables.items(), key=lambda x: x[1], reverse=True):
    status = "✅" if count > 0 else "❌"
    print(f"{status} {table:40s} : {count:>10,} rows")

print("="*70)

# Check if pipeline is healthy
bronze_count = tables["bronze_seismic_events"]
silver_count = tables["silver_seismic_events"]
exec_log_count = tables["pipeline_execution_log"]

if bronze_count > 0 and silver_count > 0 and exec_log_count > 0:
    print("🎉 PIPELINE STATUS: HEALTHY - All tables have data!")
elif exec_log_count > 0:
    print("⚠️  PIPELINE STATUS: PARTIAL - Execution logged but tables empty")
else:
    print("❌ PIPELINE STATUS: NOT RUN - No execution logs found")

print("="*70)
