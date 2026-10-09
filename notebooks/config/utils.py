# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Functions
# MAGIC **Purpose:** Reusable helper functions for all notebooks
# MAGIC
# MAGIC **Usage:** `%run ./config/utils` in any notebook

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# ==============================================================================
# TIMESTAMP HELPERS
# ==============================================================================

def utc_now():
    """
    Get current UTC timestamp (timezone-aware).
    Used for consistent timestamps across pipeline.
    """
    return datetime.now(timezone.utc)

# ==============================================================================
# LOGGING HELPERS
# ==============================================================================

def append_with_identity(df, table_name):
    """
    Insert DataFrame into table with IDENTITY columns.
    Workaround for auto-increment columns in Delta Lake.
    
    Args:
        df: DataFrame to insert
        table_name: Target table name (without database prefix)
    """
    cols = ", ".join(df.columns)
    df.createOrReplaceTempView("_tmp_append_view")
    spark.sql(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM _tmp_append_view")
    spark.catalog.dropTempView("_tmp_append_view")

def log_file_operation(layer, operation_type, file_name, batch_id, records_affected, 
                       status, file_path=None, error_details=None):
    """
    Log a file operation to file_operation_log table.
    
    Args:
        layer: STAGING / BRONZE / SILVER
        operation_type: CREATE / READ / UPDATE / DELETE
        file_name: Name of file/table
        batch_id: Batch identifier
        records_affected: Number of records
        status: SUCCESS / FAILED
        file_path: Optional full path
        error_details: Optional error message
    """
    from pyspark.sql import Row
    
    ts = utc_now()
    log_row = Row(
        operation_timestamp=ts,
        layer=layer,
        operation_type=operation_type,
        file_name=file_name,
        file_path=file_path,
        batch_id=batch_id,
        records_affected=records_affected,
        operation_status=status,
        error_details=error_details,
        created_timestamp=ts,
        updated_timestamp=ts
    )
    
    log_df = spark.createDataFrame([log_row], FILE_LOG_SCHEMA)
    append_with_identity(log_df, "file_operation_log")

def log_error(layer, error_type, error_message, batch_id=None, notebook_name=None,
              failed_record=None, stack_trace=None, resolution_status="PENDING"):
    """
    Log an error to error_log table.
    
    Args:
        layer: STAGING / BRONZE / SILVER
        error_type: VALIDATION / PARSE / CAST_FAILURE / SCHEMA_DRIFT
        error_message: Detailed error description
        batch_id: Optional batch identifier
        notebook_name: Optional notebook name
        failed_record: Optional problematic record (truncated to 4000 chars)
        stack_trace: Optional full exception trace
        resolution_status: PENDING / RESOLVED / IGNORED
    """
    from pyspark.sql import Row
    
    ts = utc_now()
    error_row = Row(
        error_timestamp=ts,
        layer=layer,
        notebook_name=notebook_name,
        batch_id=batch_id,
        error_type=error_type,
        error_message=error_message[:2000] if error_message else None,
        failed_record=failed_record[:4000] if failed_record else None,
        stack_trace=stack_trace[:8000] if stack_trace else None,
        resolution_status=resolution_status,
        resolved_by=None,
        resolution_notes=None,
        created_timestamp=ts,
        updated_timestamp=None
    )
    
    error_df = spark.createDataFrame([error_row])
    append_with_identity(error_df, "error_log")

def log_execution(start_time, end_time, notebook_name, batch_id, layer, load_type,
                  source_param, status, metrics, error_message=None):
    """
    Log pipeline execution to pipeline_execution_log table.
    
    Args:
        start_time: Execution start timestamp
        end_time: Execution end timestamp
        notebook_name: Name of notebook executed
        batch_id: Batch identifier
        layer: STAGING / BRONZE / SILVER
        load_type: FULL_LOAD / INCREMENTAL_LOAD / BACKFILL
        source_param: File/path processed
        status: SUCCESS / FAILED / PARTIAL / SKIPPED
        metrics: Dict with keys: total, inserted, updated, errored
        error_message: Optional error summary
    """
    from pyspark.sql import Row
    
    duration_sec = int((end_time - start_time).total_seconds())
    
    exec_row = Row(
        execution_start=start_time,
        execution_end=end_time,
        notebook_name=notebook_name,
        batch_id=batch_id,
        layer=layer,
        load_type=load_type,
        source_param=source_param,
        status=status,
        records_processed=int(metrics.get("total", 0)),
        records_inserted=int(metrics.get("inserted", 0)),
        records_updated=int(metrics.get("updated", 0)),
        records_errored=int(metrics.get("errored", 0)),
        error_message=error_message[:2000] if error_message else None,
        execution_duration_sec=duration_sec,
        created_timestamp=utc_now()
    )
    
    exec_df = spark.createDataFrame([exec_row])
    append_with_identity(exec_df, "pipeline_execution_log")

# ==============================================================================
# IDEMPOTENCY HELPERS
# ==============================================================================

def already_succeeded(batch_id, layer):
    """
    Check if batch_id already succeeded for given layer.
    Returns: True if batch already processed successfully
    """
    result = (spark.table("pipeline_execution_log")
              .filter((F.col("batch_id") == batch_id) & 
                     (F.col("layer") == layer) &
                     F.col("status").isin("SUCCESS", "PARTIAL"))
              .limit(1)
              .count() > 0)
    return result

# ==============================================================================
# UTILITY EXPORT
# ==============================================================================

print("✅ Utilities loaded successfully!")
print("   - Timestamp: utc_now()")
print("   - Logging: log_file_operation(), log_error(), log_execution()")
print("   - Identity insert: append_with_identity()")
print("   - Idempotency: already_succeeded()")