# Databricks notebook source
# MAGIC %md
# MAGIC # 00_Setup - Database & Table Initialization
# MAGIC 
# MAGIC **Purpose:** Create all database tables for the Seismo pipeline
# MAGIC 
# MAGIC **Run this notebook ONCE** before running any other pipeline notebooks.
# MAGIC 
# MAGIC **What it creates:**
# MAGIC - Database: `seismo`
# MAGIC - Staging Layer: `staging_raw_data`
# MAGIC - Bronze Layer: `bronze_seismic_events`
# MAGIC - Silver Layer: `silver_seismic_events`, `silver_seismic_events_quarantine`
# MAGIC - Logging Tables: `pipeline_execution_log`, `file_operation_log`, `error_log`
# MAGIC 
# MAGIC **Last Updated:** October 9, 2026

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Clean Existing Tables (TEMPORARY - Remove Before Submission)
# MAGIC 
# MAGIC ⚠️ **WARNING:** This will delete all existing tables and data!
# MAGIC 
# MAGIC **Purpose:** Clean slate for testing with new schema
# MAGIC 
# MAGIC **TODO:** Comment out or delete this entire section before final submission

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop all existing tables in correct order (handles dependencies)
# MAGIC DROP TABLE IF EXISTS silver_seismic_events_quarantine;
# MAGIC DROP TABLE IF EXISTS silver_seismic_events;
# MAGIC DROP TABLE IF EXISTS bronze_seismic_events;
# MAGIC DROP TABLE IF EXISTS bronze_error_log;
# MAGIC DROP TABLE IF EXISTS silver_error_log;
# MAGIC DROP TABLE IF EXISTS error_log;
# MAGIC DROP TABLE IF EXISTS pipeline_execution_log;
# MAGIC DROP TABLE IF EXISTS file_operation_log;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all tables are deleted
# MAGIC SHOW TABLES IN seismo;

# COMMAND ----------

print("🗑️  All existing tables deleted")
print("⚠️  REMEMBER: Remove this deletion section before submission!")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Configuration

# COMMAND ----------

%run ./config/constants

# COMMAND ----------

%run ./config/schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configure Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession

# Set timezone to UTC for consistent timestamps
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Optimize for small cluster (adjust based on your environment)
spark.conf.set("spark.sql.shuffle.partitions", "8")

print("✅ Spark configuration applied")
print(f"   Timezone: {spark.conf.get('spark.sql.session.timeZone')}")
print(f"   Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

print(f"✅ Database '{DATABASE}' created and selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Staging Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: staging_raw_data
# MAGIC Initial landing zone for raw GeoJSON data before Bronze ingestion

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STAGING_RAW_DATA} (
  staging_id          BIGINT GENERATED ALWAYS AS IDENTITY,
  event_id            STRING COMMENT 'USGS event ID (may be NULL at this stage)',
  raw_json            STRING COMMENT 'Complete GeoJSON feature as JSON string',
  source_file_name    STRING COMMENT 'Original filename',
  source_file_path    STRING COMMENT 'Full DBFS path to source file',
  file_size_bytes     BIGINT COMMENT 'Size of source file in bytes',
  batch_id            STRING COMMENT 'Batch identifier (e.g., FULL_2026-10-09)',
  ingestion_type      STRING COMMENT 'FULL_LOAD / INCREMENTAL_LOAD / BACKFILL',
  validation_status   STRING COMMENT 'VALID / INVALID / PENDING',
  validation_errors   STRING COMMENT 'JSON array of validation issues if INVALID',
  created_timestamp   TIMESTAMP COMMENT 'When record entered staging',
  updated_timestamp   TIMESTAMP COMMENT 'Last modification time'
) USING DELTA
COMMENT 'Staging layer: temporary holding zone for raw data validation'
""")

print(f"✅ Table '{STAGING_RAW_DATA}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Bronze Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: bronze_seismic_events
# MAGIC Raw data with SCD Type 2 (full historical tracking)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SEISMIC_EVENTS} (
  bronze_id           BIGINT GENERATED ALWAYS AS IDENTITY,
  event_id            STRING COMMENT 'USGS event ID (business key)',
  raw_json            STRING COMMENT 'Complete GeoJSON feature, preserved as-is',
  source_file_name    STRING COMMENT 'Source file name',
  batch_id            STRING COMMENT 'Batch identifier',
  ingestion_type      STRING COMMENT 'FULL_LOAD / INCREMENTAL_LOAD / BACKFILL',
  is_current          BOOLEAN COMMENT 'TRUE = latest version of this event',
  valid_from          TIMESTAMP COMMENT 'When this version became active',
  valid_to            TIMESTAMP COMMENT 'When superseded (NULL if current)',
  record_hash         STRING COMMENT 'MD5(raw_json) for change detection',
  created_timestamp   TIMESTAMP COMMENT 'When record was first created',
  updated_timestamp   TIMESTAMP COMMENT 'When record was last modified',
  year                INT COMMENT 'Partition column (from load timestamp)',
  month               INT COMMENT 'Partition column (from load timestamp)'
) USING DELTA
PARTITIONED BY (ingestion_type, year, month)
COMMENT 'Bronze layer: immutable raw data archive with SCD Type 2'
""")

print(f"✅ Table '{BRONZE_SEISMIC_EVENTS}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Silver Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: silver_seismic_events
# MAGIC Cleansed, typed, analytics-ready data

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SEISMIC_EVENTS} (
  event_id                STRING COMMENT 'USGS event ID (Primary Key)',
  magnitude               DOUBLE COMMENT 'Earthquake magnitude',
  magnitude_type          STRING COMMENT 'Type of magnitude (mb, ml, mw, etc.)',
  place                   STRING COMMENT 'Location description',
  longitude               DOUBLE COMMENT 'Geographic longitude',
  latitude                DOUBLE COMMENT 'Geographic latitude',
  depth_km                DOUBLE COMMENT 'Focal depth in kilometers',
  event_time              TIMESTAMP COMMENT 'UTC timestamp of earthquake',
  event_updated_time      TIMESTAMP COMMENT 'USGS last update timestamp',
  status                  STRING COMMENT 'Review status (automatic, reviewed, deleted)',
  felt_reports            INT COMMENT 'Number of Did You Feel It reports',
  cdi                     DOUBLE COMMENT 'Community Decimal Intensity',
  mmi                     DOUBLE COMMENT 'Modified Mercalli Intensity',
  alert_level             STRING COMMENT 'PAGER alert level (green, yellow, orange, red)',
  significance            INT COMMENT 'Event significance score',
  tsunami_flag            BOOLEAN COMMENT 'Whether tsunami was generated',
  network_code            STRING COMMENT 'Seismic network code',
  event_type              STRING COMMENT 'Type (earthquake, explosion, etc.)',
  is_active               BOOLEAN COMMENT 'FALSE if status=deleted (soft delete)',
  country                 STRING COMMENT 'Parsed country from place string',
  region                  STRING COMMENT 'Parsed region from place string',
  depth_category          STRING COMMENT 'Shallow / Intermediate / Deep',
  magnitude_category      STRING COMMENT 'Minor / Light / Moderate / Strong / Major / Great',
  source_batch_id         STRING COMMENT 'Reference to Bronze batch_id',
  modification_sequence   INT COMMENT 'Track change order (1, 2, 3...) for same event',
  is_most_recent_valid    BOOLEAN COMMENT 'TRUE for latest valid modification',
  created_timestamp       TIMESTAMP COMMENT 'When record was first created in Silver',
  updated_timestamp       TIMESTAMP COMMENT 'When record was last modified',
  year                    INT COMMENT 'Partition column (from event_time)',
  month                   INT COMMENT 'Partition column (from event_time)'
) USING DELTA
PARTITIONED BY (year, month)
COMMENT 'Silver layer: cleansed, typed, analytics-ready data (SCD Type 1)'
""")

print(f"✅ Table '{SILVER_SEISMIC_EVENTS}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: silver_seismic_events_quarantine
# MAGIC Bad records that failed Silver validation

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_QUARANTINE} (
  quarantine_id           BIGINT GENERATED ALWAYS AS IDENTITY,
  event_id                STRING COMMENT 'USGS event ID (if parseable)',
  batch_id                STRING COMMENT 'Source batch identifier',
  quarantine_reason       STRING COMMENT 'Why record was quarantined',
  failed_columns          STRING COMMENT 'Which columns failed validation',
  raw_json                STRING COMMENT 'Original JSON for reprocessing',
  error_details           STRING COMMENT 'Detailed error information',
  quarantine_timestamp    TIMESTAMP COMMENT 'When quarantined',
  reprocessing_status     STRING COMMENT 'PENDING / REPROCESSED / DISCARDED',
  created_timestamp       TIMESTAMP COMMENT 'Record creation time',
  updated_timestamp       TIMESTAMP COMMENT 'Record modification time'
) USING DELTA
COMMENT 'Silver quarantine: failed records awaiting investigation/reprocessing'
""")

print(f"✅ Table '{SILVER_QUARANTINE}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Operational/Logging Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: pipeline_execution_log
# MAGIC Track every pipeline execution (batch-level audit)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PIPELINE_EXECUTION_LOG} (
  execution_id            BIGINT GENERATED ALWAYS AS IDENTITY,
  execution_start         TIMESTAMP COMMENT 'Pipeline start time',
  execution_end           TIMESTAMP COMMENT 'Pipeline completion time',
  notebook_name           STRING COMMENT 'Which notebook executed',
  batch_id                STRING COMMENT 'Batch identifier',
  layer                   STRING COMMENT 'STAGING / BRONZE / SILVER',
  load_type               STRING COMMENT 'FULL_LOAD / INCREMENTAL_LOAD / BACKFILL',
  source_param            STRING COMMENT 'File/folder path processed',
  status                  STRING COMMENT 'SUCCESS / FAILED / PARTIAL / SKIPPED',
  records_processed       BIGINT COMMENT 'Total records in batch',
  records_inserted        BIGINT COMMENT 'New records added',
  records_updated         BIGINT COMMENT 'Records modified',
  records_errored         BIGINT COMMENT 'Records that failed',
  error_message           STRING COMMENT 'High-level error summary',
  execution_duration_sec  INT COMMENT 'Runtime in seconds',
  created_timestamp       TIMESTAMP COMMENT 'Log entry creation time'
) USING DELTA
COMMENT 'Pipeline execution audit log'
""")

print(f"✅ Table '{PIPELINE_EXECUTION_LOG}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: file_operation_log
# MAGIC Track all file operations (CREATE/READ/UPDATE/DELETE)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FILE_OPERATION_LOG} (
  log_id                  BIGINT GENERATED ALWAYS AS IDENTITY,
  operation_timestamp     TIMESTAMP COMMENT 'When operation occurred',
  layer                   STRING COMMENT 'STAGING / BRONZE / SILVER',
  operation_type          STRING COMMENT 'CREATE / READ / UPDATE / DELETE',
  file_name               STRING COMMENT 'File or table name',
  file_path               STRING COMMENT 'Full path to file/table',
  batch_id                STRING COMMENT 'Associated batch identifier',
  records_affected        BIGINT COMMENT 'Number of records impacted',
  operation_status        STRING COMMENT 'SUCCESS / FAILED',
  error_details           STRING COMMENT 'Error message if operation failed',
  created_timestamp       TIMESTAMP COMMENT 'Log entry creation time',
  updated_timestamp       TIMESTAMP COMMENT 'Log entry modification time'
) USING DELTA
COMMENT 'File operation audit log (tracks all file/table operations)'
""")

print(f"✅ Table '{FILE_OPERATION_LOG}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table: error_log
# MAGIC Unified error tracking across all layers

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERROR_LOG} (
  error_id                BIGINT GENERATED ALWAYS AS IDENTITY,
  error_timestamp         TIMESTAMP COMMENT 'When error occurred',
  layer                   STRING COMMENT 'STAGING / BRONZE / SILVER',
  notebook_name           STRING COMMENT 'Which notebook encountered error',
  batch_id                STRING COMMENT 'Batch being processed',
  error_type              STRING COMMENT 'VALIDATION / PARSE / CAST_FAILURE / SCHEMA_DRIFT',
  error_message           STRING COMMENT 'Detailed error description',
  failed_record           STRING COMMENT 'Problematic record (truncated)',
  stack_trace             STRING COMMENT 'Full exception trace',
  resolution_status       STRING COMMENT 'PENDING / RESOLVED / IGNORED',
  resolved_by             STRING COMMENT 'Who resolved the error',
  resolution_notes        STRING COMMENT 'How error was fixed',
  created_timestamp       TIMESTAMP COMMENT 'Error logged time',
  updated_timestamp       TIMESTAMP COMMENT 'Resolution time'
) USING DELTA
COMMENT 'Unified error log for all pipeline failures'
""")

print(f"✅ Table '{ERROR_LOG}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Validation & Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show All Created Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN seismo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Row Counts (should all be 0 initially)

# COMMAND ----------

tables = [
    STAGING_RAW_DATA,
    BRONZE_SEISMIC_EVENTS,
    SILVER_SEISMIC_EVENTS,
    SILVER_QUARANTINE,
    PIPELINE_EXECUTION_LOG,
    FILE_OPERATION_LOG,
    ERROR_LOG
]

print("📊 Initial Table Counts:")
print("=" * 70)
for table in tables:
    count = spark.table(table).count()
    print(f"   {table:40s} : {count:>10,} rows")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Describe Key Tables (verify schemas)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze_seismic_events

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_seismic_events

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Upload your data files to DBFS `/Volumes/workspace/seismo/raw/`
# MAGIC 2. Run `01_Staging_Ingestion.py` to load data into Staging
# MAGIC 3. Run `02_Bronze_Ingestion.py` to move data to Bronze
# MAGIC 4. Run `03_Silver_Transformation.py` to transform to Silver
# MAGIC 
# MAGIC **Verification:**
# MAGIC - All 7 tables created ✅
# MAGIC - All tables are empty (0 rows) ✅
# MAGIC - Database `seismo` exists ✅
# MAGIC - Partitioning applied to Bronze and Silver ✅
# MAGIC - Auto-increment IDENTITY columns configured ✅

# COMMAND ----------

print("=" * 70)
print("🎉 SETUP COMPLETE!")
print("=" * 70)
print(f"✅ Database: {DATABASE}")
print(f"✅ Tables created: {len(tables)}")
print(f"✅ Ready to run pipeline notebooks")
print("=" * 70)
print("\n📋 Next: Run 01_Staging_Ingestion.py")
