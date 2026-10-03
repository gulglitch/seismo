# Phase 2: Core Pipeline Engineering & Data Processing Design

**Project:** Seismo - Automated USGS Seismic Event Telemetry Pipeline  
**Course:** DS-3001 Data Analysis and Visualization  
**Institution:** FAST-NUCES  
**Due Date:** October 10, 2026

---

## 📋 Table of Contents

1. [Overview & Objectives](#overview--objectives)
2. [Architecture Summary](#architecture-summary)
3. [Bronze Layer Design](#bronze-layer-design)
4. [Silver Layer Design](#silver-layer-design)
5. [Gold Layer Design](#gold-layer-design)
6. [Error Logging Framework](#error-logging-framework)
7. [Schema Drift Handling Strategy](#schema-drift-handling-strategy)
8. [Slowly Changing Dimensions (SCD)](#slowly-changing-dimensions-scd)
9. [Audit & Metadata Management](#audit--metadata-management)
10. [Execution Plan & Notebooks](#execution-plan--notebooks)
11. [Decision Points (Requires Your Input)](#decision-points-requires-your-input)

---

## Overview & Objectives

Phase 2 transitions the Seismo project from data acquisition to enterprise-grade pipeline engineering. The focus is on:

- ✅ Building Bronze, Silver, and Gold layers using PySpark in Databricks
- ✅ Implementing robust error handling and logging
- ✅ Managing schema drift gracefully across layers
- ✅ Maintaining full historical lineage (SCD Type 2 pattern in Bronze)
- ✅ Ensuring idempotent execution for backfills and reruns
- ✅ Creating comprehensive audit trails

### Key Requirements from Instructor

1. **Error Log Table:** Dedicated table to capture pipeline failures, schema mismatches, and data quality issues
2. **Schema Drift Handling:** Design decisions for handling unexpected columns, type changes, and missing fields
3. **Bronze Historical Preservation:** Keep all versions of records using SCD-like markers (not just latest)
4. **Batch-Level Timestamps:** All records in a single load share the same `load_timestamp`
5. **Idempotent Execution:** Pipeline must be rerunnable without creating duplicates (in Silver/Gold)

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────┐
│  LOCAL / GITHUB                                                  │
│  ├─ usgs_earthquake_full_load.json (~200 MB)                    │
│  ├─ usgs_earthquake_sample.json (~15 MB)                        │
│  └─ usgs_earthquake_incremental.json (~1 MB)                    │
│                                                                  │
│  Upload to: DBFS /Volumes/seismo_data/raw/                      │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw + Audit)                                      │
│  ├─ Table: bronze_seismic_events                                │
│  │  - Raw GeoJSON preserved                                     │
│  │  - SCD Type 2: Keep all versions of same event              │
│  │  - Columns: event_id, raw_json, load_timestamp,             │
│  │             batch_id, is_current, valid_from, valid_to       │
│  │                                                              │
│  └─ Table: bronze_error_log                                     │
│     - Captures malformed records, parse errors                  │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleansed + Structured)                            │
│  ├─ Table: silver_seismic_events                                │
│  │  - Flattened, typed columns                                 │
│  │  - MERGE logic: Upsert on event_id                          │
│  │  - Only latest version (no duplicates)                      │
│  │  - Soft-delete support (is_active flag)                     │
│  │                                                              │
│  └─ Table: silver_error_log                                     │
│     - Schema drift issues, type casting errors                  │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Analytics Star Schema)                              │
│  ├─ FactSeismicEvent                                            │
│  ├─ DimLocation                                                 │
│  ├─ DimMagnitudeClass                                           │
│  ├─ DimTime                                                     │
│  └─ gold_error_log                                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Bronze Layer Design

### Purpose
Store raw GeoJSON with full historical lineage. This layer is **append-only** with SCD Type 2 pattern to preserve all versions of each event.

### Table: `bronze_seismic_events`

#### Schema

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `event_id` | STRING | USGS event identifier (e.g., "us6000m2x2") |
| `raw_json` | STRING | Complete GeoJSON feature as JSON string |
| `load_timestamp` | TIMESTAMP | When this batch was loaded (same for entire batch) |
| `batch_id` | STRING | Unique identifier for ingestion batch (e.g., "FULL_2026-10-03", "INCR_2026-10-04") |
| `source_file` | STRING | Origin file name (e.g., "usgs_earthquake_full_load.json") |
| `ingestion_type` | STRING | "FULL_LOAD", "INCREMENTAL_LOAD", "BACKFILL" |
| `is_current` | BOOLEAN | TRUE for the latest version of this event_id |
| `valid_from` | TIMESTAMP | When this version became active (= load_timestamp) |
| `valid_to` | TIMESTAMP | When this version was superseded (NULL if current) |
| `record_hash` | STRING | MD5 hash of raw_json (to detect actual changes) |
| `_rescued_data` | STRING | Spark's schema evolution column for unexpected fields |

#### Partitioning Strategy
```python
partitionBy("ingestion_type", "year", "month")
```
- Partitioned by ingestion type, then year/month extracted from `load_timestamp`
- Enables efficient pruning for incremental loads

#### Bronze Processing Logic

```python
# Pseudocode for Bronze ingestion
def ingest_to_bronze(source_path, batch_id, ingestion_type):
    """
    1. Read raw JSON from DBFS
    2. Add metadata columns
    3. Calculate record_hash
    4. Detect changes from existing records
    5. Update is_current flags for superseded versions
    6. Append new versions
    """
    
    # Step 1: Read raw data
    raw_df = spark.read.option("multiline", "true").json(source_path)
    
    # Step 2: Extract features array and explode
    events_df = raw_df.select(explode("features").alias("feature"))
    
    # Step 3: Add metadata
    bronze_df = events_df.select(
        col("feature.id").alias("event_id"),
        to_json(col("feature")).alias("raw_json"),
        current_timestamp().alias("load_timestamp"),
        lit(batch_id).alias("batch_id"),
        lit(source_file).alias("source_file"),
        lit(ingestion_type).alias("ingestion_type"),
        lit(True).alias("is_current"),
        current_timestamp().alias("valid_from"),
        lit(None).cast("timestamp").alias("valid_to"),
        md5(to_json(col("feature"))).alias("record_hash")
    )
    
    # Step 4: Identify existing events and check for changes
    existing_df = spark.table("bronze_seismic_events").filter(col("is_current") == True)
    
    # Step 5: Find events with actual changes (different hash)
    changed_events = bronze_df.join(
        existing_df,
        (bronze_df.event_id == existing_df.event_id) & 
        (bronze_df.record_hash != existing_df.record_hash),
        "inner"
    ).select(bronze_df.event_id)
    
    # Step 6: Update is_current and valid_to for superseded versions
    # (Use MERGE or DataFrame operations)
    
    # Step 7: Append new versions
    bronze_df.write.mode("append").partitionBy("ingestion_type", "year", "month").saveAsTable("bronze_seismic_events")
```

### Why SCD Type 2 in Bronze?

**Rationale:**
- USGS updates event magnitudes as more data becomes available
- Events can be reclassified or deleted
- Need to track "what we knew when" for audit and reprocessing
- Enables time-travel queries: "What was the magnitude on October 1st?"

**Example:**
```
Event: us6000abc1
Version 1 (Oct 1):  magnitude=4.5, status=automatic, is_current=TRUE, valid_to=NULL
Version 2 (Oct 3):  magnitude=4.7, status=reviewed,  is_current=TRUE, valid_to=NULL
                    (Version 1 updated: is_current=FALSE, valid_to='2026-10-03')
```

---

## Silver Layer Design

### Purpose
Cleansed, structured, and typed data with latest versions only. Implements **MERGE (Upsert)** pattern for idempotency.

### Table: `silver_seismic_events`

#### Schema

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `event_id` | STRING | Primary key |
| `magnitude` | DOUBLE | Earthquake magnitude |
| `magnitude_type` | STRING | Type of magnitude (mb, ml, mw, etc.) |
| `place` | STRING | Descriptive location |
| `latitude` | DOUBLE | Geographic latitude |
| `longitude` | DOUBLE | Geographic longitude |
| `depth_km` | DOUBLE | Focal depth in kilometers |
| `event_time` | TIMESTAMP | UTC timestamp of earthquake |
| `updated_time` | TIMESTAMP | Last update timestamp |
| `status` | STRING | Review status (automatic, reviewed, deleted) |
| `felt_reports` | INTEGER | Number of "Did You Feel It?" reports |
| `cdi` | DOUBLE | Community Decimal Intensity |
| `mmi` | DOUBLE | Modified Mercalli Intensity |
| `alert_level` | STRING | PAGER alert level (green, yellow, orange, red) |
| `significance` | INTEGER | Event significance score |
| `tsunami_flag` | BOOLEAN | Whether tsunami was generated |
| `network_code` | STRING | Seismic network (us, ci, nc, etc.) |
| `event_type` | STRING | Type of seismic event (earthquake, explosion, etc.) |
| `is_active` | BOOLEAN | FALSE if status='deleted' (soft delete) |
| `country` | STRING | Parsed from place string |
| `region` | STRING | Parsed from place string |
| `depth_category` | STRING | Shallow, Intermediate, Deep |
| `magnitude_category` | STRING | Minor, Light, Moderate, Strong, Major, Great |
| `load_timestamp` | TIMESTAMP | When loaded into Silver |
| `source_batch_id` | STRING | Reference to Bronze batch |

#### Partitioning Strategy
```python
partitionBy("year", "month")  # Based on event_time
```

#### Silver Processing Logic

```python
def process_bronze_to_silver(batch_id):
    """
    1. Read latest Bronze records (is_current=TRUE)
    2. Parse and flatten JSON
    3. Apply type casting with error handling
    4. Derive calculated fields
    5. MERGE into Silver (upsert on event_id)
    6. Log schema drift / casting errors to silver_error_log
    """
    
    # Step 1: Read current Bronze records for this batch
    bronze_df = spark.table("bronze_seismic_events") \
        .filter((col("batch_id") == batch_id) & (col("is_current") == True))
    
    # Step 2: Parse JSON with schema
    # DECISION POINT: Use strict schema or permissive mode?
    silver_df = bronze_df.select(
        col("event_id"),
        get_json_object("raw_json", "$.properties.mag").cast("double").alias("magnitude"),
        get_json_object("raw_json", "$.properties.magType").alias("magnitude_type"),
        get_json_object("raw_json", "$.properties.place").alias("place"),
        # ... (continue for all fields)
    )
    
    # Step 3: Derive calculated fields
    silver_enriched = silver_df.withColumn(
        "depth_category",
        when(col("depth_km") < 70, "Shallow")
        .when(col("depth_km") < 300, "Intermediate")
        .otherwise("Deep")
    ).withColumn(
        "magnitude_category",
        when(col("magnitude") < 3.0, "Minor")
        .when(col("magnitude") < 4.0, "Light")
        .when(col("magnitude") < 5.0, "Moderate")
        .when(col("magnitude") < 6.0, "Strong")
        .when(col("magnitude") < 7.0, "Major")
        .otherwise("Great")
    ).withColumn(
        "is_active",
        when(col("status") == "deleted", False).otherwise(True)
    )
    
    # Step 4: MERGE into Silver (idempotent upsert)
    # Target: existing silver_seismic_events
    # Source: silver_enriched
    # Match: event_id
    # When matched: UPDATE (if different)
    # When not matched: INSERT
    
    delta_table = DeltaTable.forName(spark, "silver_seismic_events")
    
    delta_table.alias("target").merge(
        silver_enriched.alias("source"),
        "target.event_id = source.event_id"
    ).whenMatchedUpdateAll(
        condition="target.updated_time < source.updated_time"  # Only update if newer
    ).whenNotMatchedInsertAll().execute()
```

---

## Gold Layer Design

### Purpose
Star schema optimized for BI analytics with pre-aggregated KPIs.

### Fact Table: `fact_seismic_event`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `event_key` | BIGINT | Surrogate key (auto-increment) |
| `event_id` | STRING | Natural key from USGS |
| `location_key` | INTEGER | FK to DimLocation |
| `magnitude_class_key` | INTEGER | FK to DimMagnitudeClass |
| `time_key` | INTEGER | FK to DimTime (YYYYMMDD) |
| `magnitude` | DOUBLE | Earthquake magnitude |
| `depth_km` | DOUBLE | Focal depth |
| `latitude` | DOUBLE | Latitude |
| `longitude` | DOUBLE | Longitude |
| `felt_reports` | INTEGER | DYFI report count |
| `significance` | INTEGER | Significance score |
| `tsunami_flag` | BOOLEAN | Tsunami generated |
| `seismic_energy_joules` | DOUBLE | Calculated: 10^(1.5*mag + 4.8) |
| `event_timestamp` | TIMESTAMP | Original event time |

### Dimension Table: `dim_location`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `location_key` | INTEGER | Surrogate key |
| `country` | STRING | Parsed country |
| `region` | STRING | Parsed region/province |
| `network_code` | STRING | Seismic network |

### Dimension Table: `dim_magnitude_class`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `magnitude_class_key` | INTEGER | Surrogate key |
| `magnitude_range` | STRING | "3.0-3.9", "4.0-4.9", etc. |
| `richter_category` | STRING | Minor, Light, Moderate, Strong, Major, Great |
| `typical_damage` | STRING | Description of expected damage |

### Dimension Table: `dim_time`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `time_key` | INTEGER | YYYYMMDD (e.g., 20260101) |
| `date` | DATE | Date value |
| `year` | INTEGER | Year |
| `month` | INTEGER | Month |
| `quarter` | INTEGER | Quarter |
| `day_of_week` | STRING | Monday, Tuesday, etc. |
| `is_weekend` | BOOLEAN | Weekend flag |

---

## Error Logging Framework

### Purpose
Capture and track all pipeline failures, schema mismatches, data quality issues, and execution metadata.

### Three-Tier Error Logging

#### 1. Bronze Error Log: `bronze_error_log`

**Captures:**
- Malformed JSON files
- Missing required fields (e.g., no `event_id`)
- File read errors
- Duplicate batch_id attempts

**Schema:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `error_id` | BIGINT | Auto-increment primary key |
| `error_timestamp` | TIMESTAMP | When error occurred |
| `batch_id` | STRING | Which batch failed |
| `source_file` | STRING | File being processed |
| `error_type` | STRING | "MALFORMED_JSON", "MISSING_KEY", "FILE_NOT_FOUND" |
| `error_message` | STRING | Detailed error description |
| `failed_record` | STRING | Raw content that caused error (if applicable) |
| `stack_trace` | STRING | Full exception trace |

#### 2. Silver Error Log: `silver_error_log`

**Captures:**
- Schema drift (unexpected columns)
- Type casting failures (e.g., string in magnitude field)
- Data validation failures (e.g., magnitude > 10)
- Transformation errors

**Schema:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `error_id` | BIGINT | Auto-increment primary key |
| `error_timestamp` | TIMESTAMP | When error occurred |
| `batch_id` | STRING | Source batch |
| `event_id` | STRING | USGS event ID (if parseable) |
| `layer_transition` | STRING | "BRONZE_TO_SILVER" |
| `error_type` | STRING | "SCHEMA_DRIFT", "CAST_FAILURE", "VALIDATION_ERROR" |
| `column_name` | STRING | Which column caused error |
| `expected_type` | STRING | Expected data type |
| `actual_value` | STRING | Value that failed |
| `error_message` | STRING | Detailed description |
| `resolution_action` | STRING | "QUARANTINED", "DEFAULT_APPLIED", "SKIPPED" |

#### 3. Gold Error Log: `gold_error_log`

**Captures:**
- Dimension lookup failures
- Referential integrity violations
- Aggregation errors

**Schema:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `error_id` | BIGINT | Auto-increment primary key |
| `error_timestamp` | TIMESTAMP | When error occurred |
| `event_id` | STRING | USGS event ID |
| `layer_transition` | STRING | "SILVER_TO_GOLD" |
| `error_type` | STRING | "DIMENSION_LOOKUP_FAILED", "FK_VIOLATION", "AGGREGATION_ERROR" |
| `error_message` | STRING | Detailed description |

### Error Handling Flow

```python
try:
    # Process record
    transformed_record = transform(raw_record)
    write_to_target(transformed_record)
except SchemaException as e:
    log_to_error_table(
        error_type="SCHEMA_DRIFT",
        error_message=str(e),
        failed_record=raw_record
    )
    # Apply resolution strategy (see Decision Point 2)
except CastException as e:
    log_to_error_table(
        error_type="CAST_FAILURE",
        column_name=e.column,
        expected_type=e.expected,
        actual_value=e.value
    )
```

---

## Schema Drift Handling Strategy

### What is Schema Drift?

Schema drift occurs when:
1. USGS API adds new fields (e.g., "seismic_wave_speed")
2. USGS changes data types (e.g., magnitude becomes string)
3. Fields are removed or renamed
4. Nested structure changes

### 🔴 DECISION POINT 1: Bronze Schema Approach

**Option A: Schema-on-Read (Permissive Mode)**
- **Approach:** Store raw JSON as STRING, no schema enforcement
- **Pros:** 
  - Never fails on schema changes
  - Complete data preservation
  - Flexible for future unknowns
- **Cons:** 
  - Pushes complexity to Silver layer
  - No early validation
- **Implementation:** 
  ```python
  raw_df = spark.read.option("multiline", "true").json(path)
  # Store entire feature as JSON string
  ```

**Option B: Defined Schema with mergeSchema**
- **Approach:** Define expected schema, use `mergeSchema=true` to evolve
- **Pros:** 
  - Early validation
  - Type safety in Bronze
  - Spark handles schema evolution
- **Cons:** 
  - May fail on incompatible changes (type conflicts)
  - Requires schema maintenance
- **Implementation:** 
  ```python
  defined_schema = StructType([...])
  raw_df = spark.read.schema(defined_schema).option("mergeSchema", "true").json(path)
  ```

**Option C: Spark's RESCUE_DATA Column**
- **Approach:** Define core schema, unexpected fields go to `_rescued_data`
- **Pros:** 
  - Balance between validation and flexibility
  - Automatic quarantine of drift
  - Easy to audit new fields
- **Cons:** 
  - Requires manual inspection of rescued data
- **Implementation:** 
  ```python
  raw_df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_rescued_data").json(path)
  ```

**🟢 RECOMMENDED (pending your approval): Option C**
- Gives us type safety on known fields
- Automatically captures drift without failing
- Easy to inspect and integrate new fields incrementally

---

### 🔴 DECISION POINT 2: Silver Schema Drift Resolution

When a new field appears in Bronze (e.g., "seismic_intensity_v2"), what should Silver do?

**Option A: Strict Enforcement - Reject Unknown Fields**
- **Action:** Log to error table, skip record
- **Pros:** Maintains strict schema contract
- **Cons:** Data loss, requires immediate pipeline update

**Option B: Dynamic Column Addition**
- **Action:** Automatically add new column to Silver table
- **Pros:** No data loss, pipeline continues
- **Cons:** Uncontrolled table growth, downstream breakage risk
- **Implementation:**
  ```python
  # Use Delta Lake's mergeSchema
  silver_df.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_seismic_events")
  ```

**Option C: Quarantine + Manual Review**
- **Action:** 
  1. Log new field to `silver_error_log` with error_type="SCHEMA_DRIFT"
  2. Store record with NULL for unknown field
  3. Periodically review drift log and update pipeline
- **Pros:** 
  - No data loss
  - Controlled schema evolution
  - Audit trail of changes
- **Cons:** Requires manual intervention

**Option D: Default Value Strategy**
- **Action:** Apply sensible defaults for missing fields (NULL, 0, "UNKNOWN")
- **Pros:** Pipeline never breaks
- **Cons:** May mask data quality issues

**🟢 RECOMMENDED (pending your approval): Hybrid of B + C**
- Use `mergeSchema=true` to add new columns automatically
- Log schema changes to `silver_error_log` for audit
- Set alerting for schema drift events
- Periodically review and integrate into pipeline logic

---

### 🔴 DECISION POINT 3: Type Casting Errors

What happens when magnitude="invalid_string" instead of a number?

**Option A: Fail the Batch**
- **Action:** Raise exception, stop processing
- **Pros:** Data quality guaranteed
- **Cons:** Pipeline fragility, one bad record fails entire batch

**Option B: Default Value + Log**
- **Action:** Replace with NULL or 0, log error
- **Pros:** Pipeline continues
- **Cons:** Silent data corruption if not monitored

**Option C: Quarantine Table**
- **Action:** Move failed records to `silver_quarantine` table
- **Pros:** 
  - No data loss
  - Can reprocess after fixes
  - Clean main table
- **Cons:** Additional table management

**🟢 RECOMMENDED (pending your approval): Option C**
- Create `silver_seismic_events_quarantine` table
- Failed records go there with error metadata
- Manual or automated reprocessing workflow

---

## Slowly Changing Dimensions (SCD)

### Bronze Layer: SCD Type 2 (Full History)

**Implementation:**
```python
# Track all versions with validity windows
bronze_schema = [
    "event_id",           # Business key
    "raw_json",           # Full payload
    "load_timestamp",     # When loaded
    "batch_id",           # Batch identifier
    "is_current",         # TRUE = latest version
    "valid_from",         # Start of validity (= load_timestamp)
    "valid_to",           # End of validity (NULL if current)
    "record_hash"         # MD5(raw_json) to detect changes
]

# On each load:
# 1. Check if event_id exists with different record_hash
# 2. If yes:
#    - Set is_current=FALSE, valid_to=current_timestamp for old version
#    - Insert new version with is_current=TRUE, valid_from=current_timestamp
# 3. If no change detected (same hash):
#    - Do NOT insert duplicate
```

**Query Example:**
```sql
-- Get event magnitude history
SELECT 
    event_id,
    get_json_object(raw_json, '$.properties.mag') as magnitude,
    valid_from,
    valid_to
FROM bronze_seismic_events
WHERE event_id = 'us6000abc1'
ORDER BY valid_from;

-- Get state of world on specific date
SELECT *
FROM bronze_seismic_events
WHERE is_current = FALSE 
  AND valid_from <= '2026-10-01'
  AND (valid_to > '2026-10-01' OR valid_to IS NULL);
```

### Silver Layer: Latest Version Only (SCD Type 1)

**Rationale:**
- BI tools need current state, not history
- History preserved in Bronze for audit
- MERGE ensures only one record per event_id

**Implementation:**
```python
# MERGE updates existing records in-place
# No history tracking in Silver
delta_table.alias("target").merge(
    source.alias("source"),
    "target.event_id = source.event_id"
).whenMatchedUpdate(
    condition="source.updated_time > target.updated_time",  # Only if newer
    set={
        "magnitude": "source.magnitude",
        "status": "source.status",
        # ... all columns
        "load_timestamp": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "event_id": "source.event_id",
        # ... all columns
    }
).execute()
```

---

## Audit & Metadata Management

### Pipeline Execution Log: `pipeline_execution_log`

**Purpose:** Track every pipeline run for observability

**Schema:**

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `execution_id` | BIGINT | Auto-increment |
| `execution_start` | TIMESTAMP | When pipeline started |
| `execution_end` | TIMESTAMP | When pipeline completed |
| `batch_id` | STRING | Batch being processed |
| `layer` | STRING | "BRONZE", "SILVER", "GOLD" |
| `status` | STRING | "SUCCESS", "FAILED", "PARTIAL" |
| `records_processed` | BIGINT | Total records in batch |
| `records_inserted` | BIGINT | New records added |
| `records_updated` | BIGINT | Records modified |
| `records_errored` | BIGINT | Records failed |
| `error_message` | STRING | High-level error (if failed) |
| `execution_duration_sec` | INTEGER | Runtime |

**Usage:**
```python
def log_execution(batch_id, layer, status, metrics):
    log_df = spark.createDataFrame([{
        "execution_start": start_time,
        "execution_end": current_timestamp(),
        "batch_id": batch_id,
        "layer": layer,
        "status": status,
        "records_processed": metrics["total"],
        "records_inserted": metrics["inserted"],
        "records_updated": metrics["updated"],
        "records_errored": metrics["errors"],
        "execution_duration_sec": (end_time - start_time).seconds
    }])
    log_df.write.mode("append").saveAsTable("pipeline_execution_log")
```

---

## Execution Plan & Notebooks

### Notebook Structure

#### 1. `00_Setup_Environment.py`
- Create database schemas
- Create all tables (Bronze, Silver, Gold, Error logs)
- Set up DBFS paths
- Initialize metadata tables

#### 2. `01_Bronze_Ingestion.py`
- **Parameters:** `source_path`, `batch_id`, `ingestion_type`
- Read GeoJSON from DBFS
- Apply SCD Type 2 logic
- Write to `bronze_seismic_events`
- Error handling → `bronze_error_log`

#### 3. `02_Silver_Transformation.py`
- **Parameters:** `batch_id`
- Read from Bronze (is_current=TRUE)
- Flatten JSON
- Type casting with error handling
- Derive calculated fields
- MERGE into `silver_seismic_events`
- Schema drift handling
- Errors → `silver_error_log`, quarantine table

#### 4. `03_Gold_Star_Schema.py`
- **Parameters:** `batch_id` (optional, defaults to latest Silver batch)
- Read from Silver (is_active=TRUE)
- Build dimension tables (DimLocation, DimMagnitudeClass, DimTime)
- Populate fact table (FactSeismicEvent)
- Calculate aggregations (monthly KPIs)
- Errors → `gold_error_log`

#### 5. `04_Data_Quality_Checks.py`
- Validate record counts across layers
- Check for orphan records (FK integrity)
- Identify anomalies (magnitude > 10, depth < 0)
- Generate data quality report

#### 6. `05_Error_Analysis_Dashboard.py`
- Query error logs
- Visualize schema drift patterns
- Show quarantined records
- Provide reprocessing commands

---

## Decision Points (Requires Your Input)

Please review and provide your preference for each:

### 🔴 DECISION 1: Bronze Schema Approach
- [ ] **Option A:** Schema-on-Read (store raw JSON string)
- [ ] **Option B:** Defined Schema with mergeSchema
- [X] **Option C:** RESCUE_DATA Column (RECOMMENDED)

**Your Input:** ___________

---

### 🔴 DECISION 2: Silver Schema Drift Resolution
- [ ] **Option A:** Strict Enforcement (reject unknowns)
- [X] **Option B+C:** Dynamic Addition + Logging (RECOMMENDED)
- [ ] **Option D:** Default Values

**Your Input:** ___________

---

### 🔴 DECISION 3: Type Casting Error Handling
- [ ] **Option A:** Fail the Batch
- [ ] **Option B:** Default Value + Log
- [X] **Option C:** Quarantine Table (RECOMMENDED)

**Your Input:** ___________

---

### 🔴 DECISION 4: Batch ID Format

How should we generate `batch_id`?

**Option A:** Manual Parameters
- Format: User passes "FULL_2026-10-03" or "INCR_2026-10-04"
- Pros: Full control
- Cons: Human error risk

**Option B:** Automatic Timestamp
- Format: "FULL_20261003_143022" (includes seconds)
- Pros: Automatic, unique
- Cons: Less readable

**Option C:** Hybrid
- Format: "{ingestion_type}_{date}_{sequence}" (e.g., "INCR_2026-10-04_001")
- Pros: Readable + unique
- Cons: Requires sequence tracking

**Your Input:** ___________

---

### 🔴 DECISION 5: Load Timestamp Granularity

All records in a batch share same `load_timestamp`. When should it be set?

**Option A:** Start of Batch Processing
- Timestamp captured at beginning of read
- Pros: Consistent, represents "as of" time
- Cons: May not reflect actual write time

**Option B:** End of Batch Processing
- Timestamp captured after successful write
- Pros: Accurate persistence time
- Cons: Records written at different times within batch

**Option C:** Single Timestamp Variable
- Captured once at pipeline start, reused for entire batch
- Pros: Guarantees identical value across all records
- Cons: Slight time skew if pipeline runs long

**🟢 RECOMMENDED: Option C** (ensures batch atomicity)

**Your Input:** ___________

---

### 🔴 DECISION 6: Idempotency Strategy

If we run the same batch twice, what happens?

**Option A:** Check batch_id in Bronze
- Logic: Query Bronze for existing batch_id
- If exists → Skip processing
- Pros: Simple, efficient
- Cons: Cannot reprocess failed batches

**Option B:** Overwrite Mode
- Logic: DELETE existing batch_id records, then INSERT
- Pros: True idempotency, reprocessing supported
- Cons: Breaks SCD history (need careful design)

**Option C:** Merge Logic with Batch Versioning
- Logic: Support batch_id like "INCR_2026-10-04_v2" for reruns
- Original batch preserved, new version created
- Pros: Full history + reprocessing
- Cons: Complex, table growth

**Your Input:** ___________

---

### 🔴 DECISION 7: Deleted Events in Silver

USGS marks events as deleted (status='deleted'). How do we handle in Silver?

**Option A:** Soft Delete (is_active=FALSE)
- Keep record, set flag
- Pros: Maintain referential integrity, history visible
- Cons: Query complexity (always filter is_active=TRUE)

**Option B:** Hard Delete (Physical Removal)
- DELETE from table
- Pros: Clean table
- Cons: Lost history, broken FKs

**🟢 RECOMMENDED: Option A** (aligns with Bronze history)

**Your Input:** ___________

---

### 🔴 DECISION 8: Error Log Retention Policy

Error logs can grow large. How long to keep?

**Option A:** Keep Forever
- Pros: Complete audit trail
- Cons: Storage cost

**Option B:** Rolling Window (e.g., 6 months)
- Pros: Manageable size
- Cons: Lose old error patterns

**Option C:** Aggregate + Archive
- Keep detailed logs for 3 months
- Aggregate older logs (counts only)
- Archive raw logs to cheaper storage
- Pros: Balance of detail and cost
- Cons: Implementation complexity

**Your Input:** ___________

---

## Summary & Next Steps

### Deliverables for Phase 2

1. **5 PySpark Notebooks:**
   - 00_Setup_Environment
   - 01_Bronze_Ingestion
   - 02_Silver_Transformation
   - 03_Gold_Star_Schema
   - 04_Data_Quality_Checks

2. **Data Dictionary Document** (Excel/Markdown)
   - Bronze schema
   - Silver schema  
   - Gold schemas (fact + dimensions)
   - Error log schemas

3. **Execution Guide** (README)
   - How to run full load
   - How to run incremental load
   - How to reprocess failed batches
   - How to handle schema drift

4. **Updated GitHub Repository**
   - All notebooks committed
   - Updated README.md
   - Sample execution logs

### Timeline

| Task | Duration | Target Date |
|------|----------|-------------|
| Finalize design decisions | 1 day | Oct 4 |
| Implement Bronze layer | 1 day | Oct 5 |
| Implement Silver layer | 2 days | Oct 7 |
| Implement Gold layer | 1 day | Oct 8 |
| Error handling & logging | 1 day | Oct 9 |
| Testing & documentation | 1 day | Oct 10 |

---

**Please review all decision points and provide your preferences. Once confirmed, we can proceed with implementation!**
