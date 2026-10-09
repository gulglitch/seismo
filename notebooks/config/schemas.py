# Databricks notebook source
# MAGIC %md
# MAGIC # Centralized Schema Definitions
# MAGIC **Purpose:** All PySpark schemas for the Seismo pipeline (no inferSchema anywhere)
# MAGIC 
# MAGIC **Usage:** `%run ./config/schemas` in any notebook
# MAGIC 
# MAGIC **Author:** Seismo Team - Phase 2  
# MAGIC **Last Updated:** October 9, 2026

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    ArrayType, TimestampType, BooleanType,
)

# ==============================================================================
# 1. SOURCE DATA SCHEMAS (GeoJSON from USGS)
# ==============================================================================

# ENVELOPE_SCHEMA: Top-level GeoJSON structure
# Features stored as raw STRING to preserve all fields (even unexpected ones)
ENVELOPE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("metadata", StructType([
        StructField("generated", LongType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("api", StringType(), True),
        StructField("count", IntegerType(), True),
    ]), True),
    StructField("bbox", ArrayType(DoubleType()), True),
    StructField("features", ArrayType(StringType()), True),  # Raw JSON strings
])

# FEATURE_SCHEMA: Individual earthquake event structure (for Silver parsing)
FEATURE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("properties", StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),          # Epoch milliseconds
        StructField("updated", LongType(), True),       # Epoch milliseconds
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", DoubleType(), True),
        StructField("mmi", DoubleType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", DoubleType(), True),
        StructField("rms", DoubleType(), True),
        StructField("gap", DoubleType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
    ]), True),
    StructField("geometry", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True),  # [lon, lat, depth]
    ]), True),
])

# Expected key sets for schema drift detection
EXPECTED_TOP_KEYS = [f.name for f in FEATURE_SCHEMA.fields]
EXPECTED_PROPERTY_KEYS = [f.name for f in FEATURE_SCHEMA["properties"].dataType.fields]

# ==============================================================================
# 2. STAGING LAYER SCHEMA
# ==============================================================================

STAGING_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("raw_json", StringType(), False),
    StructField("source_file_name", StringType(), False),
    StructField("source_file_path", StringType(), False),
    StructField("file_size_bytes", LongType(), True),
    StructField("batch_id", StringType(), False),
    StructField("ingestion_type", StringType(), False),
    StructField("validation_status", StringType(), False),
    StructField("validation_errors", StringType(), True),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
])

# ==============================================================================
# 3. BRONZE LAYER SCHEMA
# ==============================================================================

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("raw_json", StringType(), False),
    StructField("source_file_name", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("ingestion_type", StringType(), False),
    StructField("is_current", BooleanType(), False),
    StructField("valid_from", TimestampType(), False),
    StructField("valid_to", TimestampType(), True),
    StructField("record_hash", StringType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
])

# ==============================================================================
# 4. SILVER LAYER SCHEMA
# ==============================================================================

SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("magnitude", DoubleType(), True),
    StructField("magnitude_type", StringType(), True),
    StructField("place", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth_km", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_updated_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("felt_reports", IntegerType(), True),
    StructField("cdi", DoubleType(), True),
    StructField("mmi", DoubleType(), True),
    StructField("alert_level", StringType(), True),
    StructField("significance", IntegerType(), True),
    StructField("tsunami_flag", BooleanType(), True),
    StructField("network_code", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("depth_category", StringType(), True),
    StructField("magnitude_category", StringType(), True),
    StructField("source_batch_id", StringType(), True),
    StructField("modification_sequence", IntegerType(), True),
    StructField("is_most_recent_valid", BooleanType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
])

# ==============================================================================
# 5. OPERATIONAL / LOGGING SCHEMAS
# ==============================================================================

# Pipeline Execution Log
EXECUTION_LOG_SCHEMA = StructType([
    StructField("execution_start", TimestampType(), False),
    StructField("execution_end", TimestampType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("source_param", StringType(), True),
    StructField("status", StringType(), False),
    StructField("records_processed", LongType(), True),
    StructField("records_inserted", LongType(), True),
    StructField("records_updated", LongType(), True),
    StructField("records_errored", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_duration_sec", IntegerType(), True),
    StructField("created_timestamp", TimestampType(), False),
])

# File Operation Log (NEW - tracks all file operations)
FILE_LOG_SCHEMA = StructType([
    StructField("operation_timestamp", TimestampType(), False),
    StructField("layer", StringType(), False),
    StructField("operation_type", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("records_affected", LongType(), True),
    StructField("operation_status", StringType(), False),
    StructField("error_details", StringType(), True),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
])

# Error Log (Unified for all layers)
ERROR_LOG_SCHEMA = StructType([
    StructField("error_timestamp", TimestampType(), False),
    StructField("layer", StringType(), False),
    StructField("notebook_name", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("error_type", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("failed_record", StringType(), True),
    StructField("stack_trace", StringType(), True),
    StructField("resolution_status", StringType(), False),
    StructField("resolved_by", StringType(), True),
    StructField("resolution_notes", StringType(), True),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), True),
])

# Silver Quarantine Schema
QUARANTINE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("quarantine_reason", StringType(), False),
    StructField("failed_columns", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("error_details", StringType(), True),
    StructField("quarantine_timestamp", TimestampType(), False),
    StructField("reprocessing_status", StringType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), True),
])

# ==============================================================================
# 6. SCHEMA VALIDATION HELPERS
# ==============================================================================

def validate_schema_match(df_schema, expected_schema):
    """
    Compare DataFrame schema against expected schema.
    Returns: (is_match: bool, differences: list)
    """
    df_fields = {f.name: f.dataType for f in df_schema.fields}
    exp_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    diffs = []
    
    # Check for missing fields
    for name in exp_fields:
        if name not in df_fields:
            diffs.append(f"Missing field: {name}")
    
    # Check for extra fields
    for name in df_fields:
        if name not in exp_fields:
            diffs.append(f"Extra field: {name}")
    
    # Check for type mismatches
    for name in df_fields:
        if name in exp_fields and df_fields[name] != exp_fields[name]:
            diffs.append(f"Type mismatch for {name}: expected {exp_fields[name]}, got {df_fields[name]}")
    
    return (len(diffs) == 0, diffs)

# ==============================================================================
# SCHEMA EXPORT SUMMARY
# ==============================================================================

print("✅ Schemas loaded successfully!")
print(f"📊 Available schemas: {len([s for s in dir() if s.endswith('_SCHEMA')])} schemas defined")
print("   - ENVELOPE_SCHEMA (GeoJSON top-level)")
print("   - FEATURE_SCHEMA (individual events)")
print("   - STAGING_SCHEMA")
print("   - BRONZE_SCHEMA")
print("   - SILVER_SCHEMA")
print("   - EXECUTION_LOG_SCHEMA")
print("   - FILE_LOG_SCHEMA (NEW)")
print("   - ERROR_LOG_SCHEMA")
print("   - QUARANTINE_SCHEMA")
