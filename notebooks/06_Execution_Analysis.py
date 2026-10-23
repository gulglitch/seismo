# Databricks notebook source
# MAGIC %md
# MAGIC # Execution Plan Analysis for Phase 3 Report
# MAGIC 
# MAGIC This notebook captures Spark execution plans for the most complex transformations in the pipeline.
# MAGIC Use these outputs for the Spark UI & DAG Analysis section of your Phase 3 report.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

# Get current catalog and schema
catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
schema = spark.sql("SELECT current_schema()").collect()[0][0]

print(f"Current Catalog: {catalog}")
print(f"Current Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold Fact Table - Complex Aggregation with Joins
# MAGIC 
# MAGIC This query represents the most complex transformation: joining multiple dimensions and aggregating earthquake events.

# COMMAND ----------

# Build the query (adjust table names based on your Gold schema)
fact_query = spark.sql("""
SELECT 
    dt.date_key,
    dt.year,
    dt.month,
    dt.day_of_week,
    loc.location_key,
    loc.country,
    loc.state_province,
    loc.region,
    et.earthquake_type_key,
    et.earthquake_type,
    et.magnitude_category,
    COUNT(*) as event_count,
    AVG(e.magnitude) as avg_magnitude,
    MAX(e.magnitude) as max_magnitude,
    MIN(e.magnitude) as min_magnitude,
    AVG(e.depth_km) as avg_depth_km,
    MAX(e.depth_km) as max_depth_km,
    SUM(CASE WHEN e.tsunami_flag = 1 THEN 1 ELSE 0 END) as tsunami_events
FROM gold.fact_earthquake_events e
INNER JOIN gold.dim_time dt ON e.time_key = dt.date_key
INNER JOIN gold.dim_location loc ON e.location_key = loc.location_key
INNER JOIN gold.dim_earthquake_type et ON e.earthquake_type_key = et.earthquake_type_key
WHERE dt.year >= 2024
GROUP BY 
    dt.date_key, dt.year, dt.month, dt.day_of_week,
    loc.location_key, loc.country, loc.state_province, loc.region,
    et.earthquake_type_key, et.earthquake_type, et.magnitude_category
ORDER BY dt.date_key DESC, event_count DESC
""")

print("=" * 80)
print("EXECUTION PLAN - GOLD FACT TABLE AGGREGATION")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple Execution Plan
# MAGIC Shows the logical and physical plan in readable format

# COMMAND ----------

fact_query.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extended Execution Plan
# MAGIC Shows detailed logical plan, optimized plan, and physical plan

# COMMAND ----------

fact_query.explain(mode="extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Formatted Execution Plan (Databricks Enhanced)
# MAGIC Best for screenshots - shows tree structure clearly

# COMMAND ----------

fact_query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost-Based Execution Plan
# MAGIC Shows statistics and cost estimates

# COMMAND ----------

fact_query.explain(mode="cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Execution Metrics - Timed Run
# MAGIC 
# MAGIC Capture actual execution time and row counts

# COMMAND ----------

print("\n" + "=" * 80)
print("EXECUTION METRICS")
print("=" * 80)

start_time = time.time()

# Execute the query
result_df = fact_query
row_count = result_df.count()

end_time = time.time()
execution_time = end_time - start_time

print(f"\n✅ Query Execution Complete")
print(f"   Rows Processed: {row_count:,}")
print(f"   Execution Time: {execution_time:.2f} seconds")
print(f"   Avg Time per Row: {(execution_time / row_count * 1000):.4f} ms") if row_count > 0 else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Analyze Shuffle and Join Operations

# COMMAND ----------

# Check if broadcast join is being used for small dimension tables
print("\n" + "=" * 80)
print("BROADCAST JOIN ANALYSIS")
print("=" * 80)

# Check dimension table sizes
dim_tables = ['gold.dim_time', 'gold.dim_location', 'gold.dim_earthquake_type']

for table in dim_tables:
    try:
        row_count = spark.table(table).count()
        print(f"\n{table}: {row_count:,} rows")
        
        # Estimate size
        if row_count < 10000:
            print(f"   ✅ Small enough for broadcast join (< 10K rows)")
        else:
            print(f"   ⚠️  May trigger shuffle join (> 10K rows)")
    except Exception as e:
        print(f"   ❌ Table not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Optimization Evidence - Partitioning Strategy

# COMMAND ----------

print("\n" + "=" * 80)
print("PARTITION ANALYSIS")
print("=" * 80)

# Show partitioning info for fact table
try:
    spark.sql("DESCRIBE EXTENDED gold.fact_earthquake_events").filter(
        col("col_name").contains("Partition")
    ).show(truncate=False)
except Exception as e:
    print(f"Could not retrieve partition info: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Complex Silver to Gold Transformation

# COMMAND ----------

# Analyze the Silver to Gold transformation
silver_to_gold_query = spark.sql("""
SELECT 
    id,
    magnitude,
    place,
    event_time,
    updated_time,
    timezone_offset,
    url,
    detail_url,
    felt_reports,
    cdi,
    mmi,
    alert_level,
    status_type,
    tsunami_flag,
    significance,
    network,
    code,
    ids,
    sources,
    event_types,
    num_stations_used,
    horizontal_error_km,
    depth_error_km,
    magnitude_error,
    magnitude_num_stations,
    review_status,
    location_source,
    magnitude_source,
    longitude,
    latitude,
    depth_km,
    earthquake_type,
    magnitude_category,
    depth_category,
    country,
    state_province,
    region,
    YEAR(event_time) as year,
    MONTH(event_time) as month,
    DAY(event_time) as day,
    HOUR(event_time) as hour,
    ingestion_timestamp
FROM silver.earthquake_events
WHERE event_time >= date_sub(current_date(), 30)
""")

print("\n" + "=" * 80)
print("SILVER TO GOLD TRANSFORMATION PLAN")
print("=" * 80)

silver_to_gold_query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Screenshots for Report
# MAGIC 
# MAGIC **Instructions:**
# MAGIC 1. Run all cells above
# MAGIC 2. Take screenshots of:
# MAGIC    - Cell 6: Formatted execution plan (best visual)
# MAGIC    - Cell 8: Execution metrics (shows performance)
# MAGIC    - Cell 9: Broadcast join analysis (shows optimization awareness)
# MAGIC    - Cell 10: Partition analysis (shows physical optimization)
# MAGIC 
# MAGIC **For your report, explain:**
# MAGIC - **Stages**: How many stages Spark created
# MAGIC - **Shuffles**: Where shuffles occur (usually at GROUP BY and large JOINs)
# MAGIC - **Broadcast Joins**: If small dimension tables are broadcast
# MAGIC - **Predicate Pushdown**: WHERE filters applied early in the plan
# MAGIC - **Partition Pruning**: If partitioning reduces data scanned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Concepts to Mention in Your Report
# MAGIC 
# MAGIC ### 1. **Broadcast Join**
# MAGIC - Small dimension tables (< 10MB) are broadcast to all executors
# MAGIC - Avoids expensive shuffle operations
# MAGIC - Look for `BroadcastHashJoin` in the plan
# MAGIC 
# MAGIC ### 2. **Shuffle**
# MAGIC - Data redistribution across executors
# MAGIC - Occurs at GROUP BY and large JOINs
# MAGIC - Look for `Exchange` operations in the plan
# MAGIC 
# MAGIC ### 3. **Predicate Pushdown**
# MAGIC - WHERE filters applied before joins
# MAGIC - Reduces data volume early
# MAGIC - Look for `Filter` operations near the bottom (early) in the plan
# MAGIC 
# MAGIC ### 4. **Partition Pruning**
# MAGIC - Only reads relevant partitions based on filter
# MAGIC - Reduces I/O significantly
# MAGIC - Check `PartitionFilters` in the plan
# MAGIC 
# MAGIC ### 5. **Aggregation Strategy**
# MAGIC - Partial aggregation at executor level
# MAGIC - Final aggregation after shuffle
# MAGIC - Look for `HashAggregate` operations
