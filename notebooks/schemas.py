# Databricks notebook source
# MAGIC %md
# MAGIC # schemas — explicit StructTypes for Seismo (no inferSchema anywhere)
# MAGIC Used via `%run ./schemas` in the Bronze and Silver notebooks.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    ArrayType, TimestampType, BooleanType,
)

# ---------------------------------------------------------------------------
# 1) BRONZE envelope schema  (used with spark.read.schema(...).json(...))
# ---------------------------------------------------------------------------
# `features` is ARRAY<STRING> on purpose: Spark's JSON reader hands back each
# feature object as its raw JSON text, so raw_json is preserved byte-for-byte
# (unexpected fields included). Parsing it into a struct and calling to_json()
# would silently drop any field that is not in the schema.
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
    StructField("features", ArrayType(StringType()), True),
])

# ---------------------------------------------------------------------------
# 2) SILVER feature schema  (the *expected* shape of one USGS GeoJSON feature)
# ---------------------------------------------------------------------------
# USGS time/updated are epoch MILLISECONDS (long) -> cast to timestamp in Silver.
FEATURE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("properties", StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
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
        StructField("coordinates", ArrayType(DoubleType()), True),  # [lon, lat, depth_km]
    ]), True),
])

# Expected key sets — Silver compares json_object_keys(raw_json) against these
# to detect schema drift (new / removed fields).
EXPECTED_TOP_KEYS = [f.name for f in FEATURE_SCHEMA.fields]
EXPECTED_PROPERTY_KEYS = [f.name for f in FEATURE_SCHEMA["properties"].dataType.fields]

# ---------------------------------------------------------------------------
# 3) Operational table schemas (explicit, so log writes never infer either)
# ---------------------------------------------------------------------------
EXECUTION_LOG_SCHEMA = StructType([
    StructField("execution_start", TimestampType(), False),
    StructField("execution_end", TimestampType(), False),
    StructField("batch_id", StringType(), False),
    StructField("layer", StringType(), False),            # RAW_TO_BRONZE | BRONZE_TO_SILVER | SILVER_TO_GOLD
    StructField("load_type", StringType(), False),        # FULL_LOAD | INCREMENTAL_LOAD | BACKFILL
    StructField("source_param", StringType(), True),      # file / folder / date parameter processed
    StructField("status", StringType(), False),           # SUCCESS | PARTIAL | FAILED | SKIPPED
    StructField("records_processed", LongType(), True),
    StructField("records_inserted", LongType(), True),
    StructField("records_updated", LongType(), True),
    StructField("records_errored", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_duration_sec", IntegerType(), True),
])

BRONZE_ERROR_SCHEMA = StructType([
    StructField("error_timestamp", TimestampType(), False),
    StructField("batch_id", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("error_type", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("failed_record", StringType(), True),
    StructField("stack_trace", StringType(), True),
])
