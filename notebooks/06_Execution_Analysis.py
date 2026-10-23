# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Execution Plan Analysis for Phase 3 Report
# MAGIC
# MAGIC This notebook captures Spark execution plans for the most complex transformations in the pipeline.
# MAGIC Use these outputs for the Spark UI & DAG Analysis section of your Phase 3 report.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./config/constants

# COMMAND ----------

# MAGIC %run ./config/schemas

# COMMAND ----------

# MAGIC %run ./config/utils

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

# Set the database context to 'seismo'
spark.sql(f"USE SCHEMA {DATABASE}")

# Verify current context
catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
schema = spark.sql("SELECT current_schema()").collect()[0][0]

print(f"✅ Current Catalog: {catalog}")
print(f"✅ Current Schema: {schema}")
print(f"✅ Database configured: {DATABASE}")

# Verify Gold tables exist
gold_tables_check = spark.sql("SHOW TABLES").filter("tableName LIKE 'gold_%'")
print(f"\n📊 Gold tables found: {gold_tables_check.count()}")
gold_tables_check.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold Fact Table - Complex Aggregation with Joins
# MAGIC
# MAGIC This query represents the most complex transformation: joining multiple dimensions and aggregating earthquake events.

# COMMAND ----------

# Build the query based on YOUR actual Gold schema
fact_query = spark.sql("""
SELECT 
    dt.date_key,
    dt.year,
    dt.month,
    dt.day_of_week,
    loc.location_key,
    loc.country,
    loc.state_province,
    loc.location_label,
    mc.magnitude_class_key,
    mc.magnitude_class,
    mc.magnitude_range,
    dc.depth_class_key,
    dc.depth_class,
    COUNT(*) as event_count,
    AVG(e.magnitude) as avg_magnitude,
    MAX(e.magnitude) as max_magnitude,
    MIN(e.magnitude) as min_magnitude,
    AVG(e.depth_km) as avg_depth_km,
    MAX(e.depth_km) as max_depth_km,
    SUM(CASE WHEN e.tsunami_flag = true THEN 1 ELSE 0 END) as tsunami_events,
    SUM(e.energy_joules) as total_energy_joules
FROM gold_fact_seismic_event e
INNER JOIN gold_dim_date dt ON e.date_key = dt.date_key
INNER JOIN gold_dim_location loc ON e.location_key = loc.location_key
INNER JOIN gold_dim_magnitude_class mc ON e.magnitude_class_key = mc.magnitude_class_key
INNER JOIN gold_dim_depth_class dc ON e.depth_class_key = dc.depth_class_key
WHERE dt.year >= 2024 AND e.is_active = true
GROUP BY 
    dt.date_key, dt.year, dt.month, dt.day_of_week,
    loc.location_key, loc.country, loc.state_province, loc.location_label,
    mc.magnitude_class_key, mc.magnitude_class, mc.magnitude_range,
    dc.depth_class_key, dc.depth_class
ORDER BY dt.date_key DESC, event_count DESC
""")

print("=" * 80)
print("EXECUTION PLAN - GOLD FACT TABLE AGGREGATION")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📸 SINGLE SCREENSHOT FOR REPORT - Formatted Execution Plan
# MAGIC
# MAGIC **This is the ONLY cell you need to screenshot for your Phase 3 report.**
# MAGIC
# MAGIC It shows the physical execution plan with clear tree structure.

# COMMAND ----------

print("=" * 100)
print("SPARK PHYSICAL EXECUTION PLAN - Gold Fact Aggregation Query")
print("=" * 100)
print("\nQuery: Multi-dimensional aggregation with 4 dimension joins and GROUP BY")
print("Tables: gold_fact_seismic_event + 4 dimension tables")
print("=" * 100)
print()

fact_query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Execution Metrics Summary
# MAGIC
# MAGIC Capture performance metrics to include in your report text (not a separate screenshot).

# COMMAND ----------

print("\n" + "=" * 80)
print("EXECUTION METRICS FOR REPORT")
print("=" * 80)

start_time = time.time()

# Execute the query
result_df = fact_query
row_count = result_df.count()

end_time = time.time()
execution_time = end_time - start_time

print(f"\n✅ Query Execution Complete")
print(f"   Rows Returned: {row_count:,}")
print(f"   Execution Time: {execution_time:.2f} seconds")
print(f"   Throughput: {(row_count / execution_time):,.0f} rows/second") if execution_time > 0 else None


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optimization Evidence

# COMMAND ----------

print("\n" + "=" * 80)
print("DIMENSION TABLE ANALYSIS (for Broadcast Join validation)")
print("=" * 80)

# Check dimension table sizes
dim_tables = ['gold_dim_date', 'gold_dim_location', 'gold_dim_magnitude_class', 'gold_dim_depth_class']

print("\n✅ Small dimensions (< 10K rows) qualify for broadcast joins:")
for table in dim_tables:
    try:
        row_count = spark.table(table).count()
        status = "✅ Broadcast eligible" if row_count < 10000 else "⚠️  May trigger shuffle"
        print(f"   {table}: {row_count:,} rows - {status}")
    except Exception as e:
        print(f"   {table}: ❌ Not found")
