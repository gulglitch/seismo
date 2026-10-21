# Databricks notebook source
# MAGIC %md
# MAGIC # Constants & Configuration
# MAGIC **Purpose:** Centralized configuration values (database names, table names, paths)
# MAGIC
# MAGIC **Usage:** `%run ./config/constants` in any notebook

# COMMAND ----------

# ==============================================================================
# DATABASE & SCHEMA NAMES
# ==============================================================================

DATABASE = "seismo"
CATALOG = "hive_metastore"  # Change if using Unity Catalog

# ==============================================================================
# TABLE NAMES
# ==============================================================================

# Bronze Layer
BRONZE_SEISMIC_EVENTS = "bronze_seismic_events"

# Silver Layer
SILVER_SEISMIC_EVENTS = "silver_seismic_events"
SILVER_QUARANTINE = "silver_seismic_events_quarantine"

# Gold Layer (Star Schema)
GOLD_DIM_DATE = "gold_dim_date"
GOLD_DIM_LOCATION = "gold_dim_location"
GOLD_DIM_MAGNITUDE_CLASS = "gold_dim_magnitude_class"
GOLD_DIM_DEPTH_CLASS = "gold_dim_depth_class"
GOLD_FACT_SEISMIC_EVENT = "gold_fact_seismic_event"
GOLD_FACT_DAILY_SUMMARY = "gold_fact_daily_location_summary"
GOLD_TABLES = [GOLD_DIM_DATE, GOLD_DIM_LOCATION, GOLD_DIM_MAGNITUDE_CLASS,
               GOLD_DIM_DEPTH_CLASS, GOLD_FACT_SEISMIC_EVENT, GOLD_FACT_DAILY_SUMMARY]

# Gold BI views over the audit tables
GOLD_VW_PIPELINE_RUNS = "gold_vw_pipeline_runs"
GOLD_VW_ERROR_SUMMARY = "gold_vw_error_summary"
GOLD_VW_QUARANTINE_SUMMARY = "gold_vw_quarantine_summary"

# Operational Tables
PIPELINE_EXECUTION_LOG = "pipeline_execution_log"
FILE_OPERATION_LOG = "file_operation_log"
ERROR_LOG = "error_log"

# ==============================================================================
# LAYER NAMES (for logging)
# ==============================================================================

LAYER_BRONZE = "BRONZE"
LAYER_SILVER = "SILVER"
LAYER_GOLD = "GOLD"

# ==============================================================================
# LOAD TYPES
# ==============================================================================

LOAD_TYPE_FULL = "FULL_LOAD"
LOAD_TYPE_INCREMENTAL = "INCREMENTAL_LOAD"
LOAD_TYPE_BACKFILL = "BACKFILL"

# ==============================================================================
# BATCH ID PREFIXES
# ==============================================================================

PREFIX_FULL = "FULL"
PREFIX_INCR = "INCR"
PREFIX_BACKFILL = "BACKFILL"

# ==============================================================================
# STATUS VALUES
# ==============================================================================

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_PARTIAL = "PARTIAL"
STATUS_SKIPPED = "SKIPPED"

# ==============================================================================
# FILE OPERATION TYPES
# ==============================================================================

OP_CREATE = "CREATE"
OP_READ = "READ"
OP_UPDATE = "UPDATE"
OP_DELETE = "DELETE"

# ==============================================================================
# ERROR TYPES
# ==============================================================================

ERROR_VALIDATION = "VALIDATION"
ERROR_PARSE = "PARSE"
ERROR_CAST = "CAST_FAILURE"
ERROR_SCHEMA_DRIFT = "SCHEMA_DRIFT"
ERROR_FILE_NOT_FOUND = "FILE_NOT_FOUND"
ERROR_MALFORMED_JSON = "MALFORMED_JSON"
ERROR_MISSING_KEY = "MISSING_KEY"
ERROR_DUPLICATE = "DUPLICATE"

# ==============================================================================
# RESOLUTION STATUS
# ==============================================================================

RESOLUTION_PENDING = "PENDING"
RESOLUTION_RESOLVED = "RESOLVED"
RESOLUTION_IGNORED = "IGNORED"

# ==============================================================================
# VALIDATION STATUS
# ==============================================================================

VALIDATION_VALID = "VALID"
VALIDATION_INVALID = "INVALID"
VALIDATION_PENDING = "PENDING"

# ==============================================================================
# REPROCESSING STATUS
# ==============================================================================

REPROCESS_PENDING = "PENDING"
REPROCESS_DONE = "REPROCESSED"
REPROCESS_DISCARD = "DISCARDED"

# ==============================================================================
# NOTEBOOK NAMES (for execution tracking)
# ==============================================================================

NOTEBOOK_SETUP = "00_Setup"
NOTEBOOK_BRONZE = "02_Bronze_Ingestion"
NOTEBOOK_SILVER = "03_Silver_Transformation"
NOTEBOOK_QUALITY = "04_Data_Quality_Checks"
NOTEBOOK_BACKFILL = "05_Backfill_Orchestrator"
NOTEBOOK_GOLD = "03_Gold_Transformations"
NOTEBOOK_GOLD_OPTIMIZE = "04_Gold_Optimize"
NOTEBOOK_PIPELINE = "05_Run_Pipeline"

# Unknown member surrogate key used by every Gold dimension
GOLD_UNKNOWN_KEY = -1

# ==============================================================================
# DEFAULT PATHS (update these based on your DBFS setup)
# ==============================================================================

# Source data location
SOURCE_BASE_PATH = "/Volumes/workspace/seismo/raw"
SOURCE_FULL_LOAD = f"{SOURCE_BASE_PATH}/usgs_earthquake_full_load_2019_2024.json"
SOURCE_INCREMENTAL = f"{SOURCE_BASE_PATH}/usgs_earthquake_incremental.json"
SOURCE_SAMPLE = f"{SOURCE_BASE_PATH}/usgs_earthquake_sample.json"

# ==============================================================================
# SPARK CONFIGURATION
# ==============================================================================

SPARK_TIMEZONE = "UTC"
SPARK_SHUFFLE_PARTITIONS = 8  # Adjust based on cluster size

# ==============================================================================
# DEPTH CATEGORIES (for Silver enrichment)
# ==============================================================================

DEPTH_SHALLOW = "Shallow"      # < 70 km
DEPTH_INTERMEDIATE = "Intermediate"  # 70-300 km
DEPTH_DEEP = "Deep"            # > 300 km

# ==============================================================================
# MAGNITUDE CATEGORIES (Richter Scale)
# ==============================================================================

MAG_MINOR = "Minor"            # < 3.0
MAG_LIGHT = "Light"            # 3.0-3.9
MAG_MODERATE = "Moderate"      # 4.0-4.9
MAG_STRONG = "Strong"          # 5.0-5.9
MAG_MAJOR = "Major"            # 6.0-6.9
MAG_GREAT = "Great"            # >= 7.0

# ==============================================================================
# ALERT LEVELS
# ==============================================================================

ALERT_GREEN = "green"
ALERT_YELLOW = "yellow"
ALERT_ORANGE = "orange"
ALERT_RED = "red"

# ==============================================================================
# HELPER: Generate Batch ID
# ==============================================================================

def make_batch_id(load_type, run_date):
    """
    Generate standardized batch_id.
    Examples: FULL_2026-10-09, INCR_2026-10-10, BACKFILL_2026-09-01
    """
    prefix_map = {
        LOAD_TYPE_FULL: PREFIX_FULL,
        LOAD_TYPE_INCREMENTAL: PREFIX_INCR,
        LOAD_TYPE_BACKFILL: PREFIX_BACKFILL
    }
    prefix = prefix_map.get(load_type, "BATCH")
    return f"{prefix}_{run_date}"

# ==============================================================================
# CONFIGURATION EXPORT
# ==============================================================================

print("✅ Constants loaded successfully!")
print(f"📦 Database: {DATABASE}")
print(f"📊 Tables: {len([t for t in dir() if t.isupper() and not t.startswith('_')])} constants defined")
print(f"📁 Source path: {SOURCE_BASE_PATH}")