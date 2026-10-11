# 🌍 Seismo: Automated USGS Seismic Event Telemetry Pipeline

**DS-3001 Data Engineering Project | FAST-NUCES**

> Global earthquake monitoring and analytics using Databricks Medallion Architecture

---

## 📋 Project Overview

This project implements an **enterprise-grade data engineering pipeline** for global seismic event monitoring. The system ingests earthquake data from the USGS (United States Geological Survey) and processes it through a **Bronze → Silver** Medallion architecture to provide clean, analytics-ready data for disaster risk analysis and geological research.

### Key Features

- ✅ **Strict schema enforcement** (no `inferSchema`) using PySpark StructType
- ✅ **Bronze layer with SCD Type 2** for complete historical tracking
- ✅ **Idempotent execution** with hash-based change detection
- ✅ **Delta Lake** for ACID transactions and time travel capabilities
- ✅ **Comprehensive audit framework** with execution, file operation, and error logging
- ✅ **Schema drift detection** with automatic quarantine
- ✅ **Robust error handling** across all pipeline stages

---

## 🎯 Project Goals

1. Build a production-grade lakehouse architecture on Databricks
2. Implement SCD Type 2 pattern for historical event tracking
3. Ensure pipeline idempotency for safe re-runs and backfills
4. Handle schema drift and data quality issues gracefully
5. Maintain comprehensive audit trails for all operations

---

## 📊 Data Source

**API:** [USGS FDSN Event Web Service](https://earthquake.usgs.gov/fdsnws/event/1/query)

- **Format:** GeoJSON
- **Authentication:** None (public API)
- **Data Quality:** Highly reliable, maintained by U.S. government
- **Update Frequency:** Real-time (events updated as seismologists re-evaluate)

### Sample Data Schema

```json
{
  "type": "Feature",
  "id": "us6000m2x2",
  "properties": {
    "mag": 4.6,
    "place": "180 km SW of Merizo Village, Guam",
    "time": 1704067154000,
    "updated": 1704068000000,
    "status": "reviewed",
    "felt": 52,
    "cdi": 3.2,
    "mmi": 4.1,
    "alert": "green",
    "sig": 312,
    "magType": "mb",
    "type": "earthquake",
    "tsunami": 0
  },
  "geometry": {
    "type": "Point",
    "coordinates": [143.3492, 12.2725, 30.12]
  }
}
```

---

## 🗄️ Data Models

### **Bronze Layer: `bronze_seismic_events`**

**Purpose:** Raw data landing zone with SCD Type 2 (full historical tracking)

**Primary Key:** `(event_id, valid_from)` — Composite key for versioning

**Partitioning:** `PARTITIONED BY (ingestion_type, year, month)`

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `event_id` | STRING | No | USGS event identifier (business key) |
| `raw_json` | STRING | No | Complete GeoJSON feature, preserved as-is |
| `source_file_name` | STRING | Yes | Name of source file processed |
| `batch_id` | STRING | No | Batch identifier (e.g., FULL_2026-10-09, INCR_2026-10-10) |
| `ingestion_type` | STRING | No | FULL_LOAD / INCREMENTAL_LOAD / BACKFILL |
| `is_current` | BOOLEAN | No | TRUE = latest version of this event_id |
| `valid_from` | TIMESTAMP | No | When this version became active (SCD2 start) |
| `valid_to` | TIMESTAMP | Yes | When version was superseded (NULL if current) |
| `record_hash` | STRING | No | MD5(raw_json) for change detection |
| `created_timestamp` | TIMESTAMP | No | When record was first created |
| `updated_timestamp` | TIMESTAMP | No | When record was last modified |
| `year` | INT | Yes | Partition column (from valid_from) |
| `month` | INT | Yes | Partition column (from valid_from) |

**Key Features:**
- ✅ Explicit schema enforcement via `ENVELOPE_SCHEMA` (no `inferSchema`)
- ✅ SCD Type 2: Maintains complete version history of every event
- ✅ Idempotent: Re-running same data inserts 0 rows (hash-based deduplication)
- ✅ Change detection: Only events with different `record_hash` create new versions
- ✅ Point-in-time queries: Reconstruct data state at any past timestamp

**Example: Event Version History**
```
Event: us6000abc1
Version 1 (Oct 1):  mag=4.5, status=automatic, is_current=TRUE, valid_to=NULL
Version 2 (Oct 3):  mag=4.7, status=reviewed,  is_current=TRUE, valid_to=NULL
                    (Version 1 updated: is_current=FALSE, valid_to='2026-10-03')
```

---

### **Silver Layer: `silver_seismic_events`**

**Purpose:** Cleansed, typed, analytics-ready data (SCD Type 1 - latest version only)

**Primary Key:** `event_id` — Unique USGS event identifier

**Partitioning:** `PARTITIONED BY (year, month)` — Based on `event_time` for time-series queries

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `event_id` | STRING | No | USGS event identifier (PK) |
| `magnitude` | DOUBLE | Yes | Earthquake magnitude |
| `magnitude_type` | STRING | Yes | Magnitude scale (mb, ml, mw, md, etc.) |
| `place` | STRING | Yes | Location description |
| `longitude` | DOUBLE | Yes | Geographic longitude (-180 to 180) |
| `latitude` | DOUBLE | Yes | Geographic latitude (-90 to 90) |
| `depth_km` | DOUBLE | Yes | Focal depth in kilometers |
| `event_time` | TIMESTAMP | Yes | UTC timestamp of earthquake occurrence |
| `event_updated_time` | TIMESTAMP | Yes | USGS last revision timestamp (drives MERGE logic) |
| `status` | STRING | Yes | Review status (automatic, reviewed, deleted) |
| `felt_reports` | INT | Yes | Number of "Did You Feel It?" reports |
| `cdi` | DOUBLE | Yes | Community Decimal Intensity |
| `mmi` | DOUBLE | Yes | Modified Mercalli Intensity |
| `alert_level` | STRING | Yes | PAGER alert level (green, yellow, orange, red) |
| `significance` | INT | Yes | Event significance score |
| `tsunami_flag` | BOOLEAN | Yes | Whether tsunami was generated |
| `network_code` | STRING | Yes | Seismic network code (us, ci, ak, etc.) |
| `event_type` | STRING | Yes | Event classification (earthquake, explosion, etc.) |
| `is_active` | BOOLEAN | No | FALSE if status='deleted' (soft delete) |
| `country` | STRING | Yes | Parsed country from place string |
| `region` | STRING | Yes | Parsed region/locality from place string |
| `depth_category` | STRING | Yes | Shallow (<70km) / Intermediate (70-300km) / Deep (>300km) |
| `magnitude_category` | STRING | Yes | Minor/Light/Moderate/Strong/Major/Great (Richter scale) |
| `source_batch_id` | STRING | Yes | Reference to Bronze batch_id |
| `modification_sequence` | INT | Yes | Tracks update count for this event (1, 2, 3...) |
| `is_most_recent_valid` | BOOLEAN | No | TRUE for latest valid modification |
| `created_timestamp` | TIMESTAMP | No | When first created in Silver |
| `updated_timestamp` | TIMESTAMP | No | When last modified in Silver |
| `year` | INT | Yes | Partition column (from event_time) |
| `month` | INT | Yes | Partition column (from event_time) |

**Key Features:**
- ✅ Type enforcement: All epoch timestamps converted via `from_unixtime(ms/1000)`
- ✅ Data quality: Invalid records quarantined (lat/lon bounds, magnitude limits)
- ✅ Schema drift detection: Unexpected JSON keys logged to `error_log`
- ✅ Idempotent MERGE: Updates only if `event_updated_time` > current value
- ✅ Enrichment: Derived columns (country/region parsing, depth/magnitude categories)

---

### **Quarantine Table: `silver_seismic_events_quarantine`**

**Purpose:** Failed records awaiting investigation/reprocessing

| Column | Data Type | Description |
|--------|-----------|-------------|
| `quarantine_id` | BIGINT | Auto-increment identity |
| `event_id` | STRING | USGS event ID (if parseable) |
| `batch_id` | STRING | Source batch identifier |
| `quarantine_reason` | STRING | Why record was quarantined |
| `failed_columns` | STRING | Which columns failed validation |
| `raw_json` | STRING | Original JSON for reprocessing |
| `error_details` | STRING | Detailed error information |
| `quarantine_timestamp` | TIMESTAMP | When quarantined |
| `reprocessing_status` | STRING | PENDING / REPROCESSED / DISCARDED |
| `created_timestamp` | TIMESTAMP | Record creation time |
| `updated_timestamp` | TIMESTAMP | Record modification time |

---

### **Operational Tables**

#### **`pipeline_execution_log`** — Batch-level execution tracking

| Column | Data Type | Description |
|--------|-----------|-------------|
| `execution_id` | BIGINT | Auto-increment identity |
| `execution_start` | TIMESTAMP | Pipeline start time |
| `execution_end` | TIMESTAMP | Pipeline completion time |
| `notebook_name` | STRING | Which notebook executed |
| `batch_id` | STRING | Batch identifier |
| `layer` | STRING | RAW_TO_BRONZE / BRONZE_TO_SILVER |
| `load_type` | STRING | FULL_LOAD / INCREMENTAL_LOAD / BACKFILL |
| `source_param` | STRING | File/path processed |
| `status` | STRING | SUCCESS / FAILED / PARTIAL / SKIPPED |
| `records_processed` | BIGINT | Total records in batch |
| `records_inserted` | BIGINT | New records added |
| `records_updated` | BIGINT | Records modified (Bronze: versions expired) |
| `records_errored` | BIGINT | Records that failed |
| `error_message` | STRING | High-level error summary |
| `execution_duration_sec` | INT | Runtime in seconds |
| `created_timestamp` | TIMESTAMP | Log entry creation time |

#### **`file_operation_log`** — File-level audit trail

| Column | Data Type | Description |
|--------|-----------|-------------|
| `log_id` | BIGINT | Auto-increment identity |
| `operation_timestamp` | TIMESTAMP | When operation occurred |
| `layer` | STRING | BRONZE / SILVER |
| `operation_type` | STRING | CREATE / READ / UPDATE / DELETE |
| `file_name` | STRING | File or table name |
| `file_path` | STRING | Full path to file/table |
| `batch_id` | STRING | Associated batch identifier |
| `records_affected` | BIGINT | Number of records impacted |
| `operation_status` | STRING | SUCCESS / FAILED |
| `error_details` | STRING | Error message if operation failed |
| `created_timestamp` | TIMESTAMP | Log entry creation time |
| `updated_timestamp` | TIMESTAMP | Log entry modification time |

#### **`error_log`** — Unified error tracking

| Column | Data Type | Description |
|--------|-----------|-------------|
| `error_id` | BIGINT | Auto-increment identity |
| `error_timestamp` | TIMESTAMP | When error occurred |
| `layer` | STRING | BRONZE / SILVER |
| `notebook_name` | STRING | Which notebook encountered error |
| `batch_id` | STRING | Batch being processed |
| `error_type` | STRING | VALIDATION / PARSE / CAST_FAILURE / SCHEMA_DRIFT |
| `error_message` | STRING | Detailed error description |
| `failed_record` | STRING | Problematic record (truncated) |
| `stack_trace` | STRING | Full exception trace |
| `resolution_status` | STRING | PENDING / RESOLVED / IGNORED |
| `resolved_by` | STRING | Who resolved the error |
| `resolution_notes` | STRING | How error was fixed |
| `created_timestamp` | TIMESTAMP | Error logged time |
| `updated_timestamp` | TIMESTAMP | Resolution time |

---

## 🚀 Execution Guide

### **Prerequisites**

1. **Databricks Workspace** (Community Edition or Azure with student credits)
2. **DBFS Access** for file storage
3. **Unity Catalog or Hive Metastore** configured

---

### **Initial Setup**

#### **Step 1: Run Setup Notebook**
```python
# Notebook: 00_Setup.py
# Purpose: Create all tables and configure database

# No parameters required
# Creates:
# - Database: seismo
# - 6 tables: Bronze, Silver, Quarantine, and 3 logging tables
# - Sets up partitioning and schema enforcement
```

**Expected Output:**
- ✅ All 6 tables created
- ✅ Database `seismo` exists
- ✅ All tables empty (0 rows initially)

---

#### **Step 2: Fetch Data from USGS API**
```python
# Notebook: 00_Fetch_USGS_Data.py
# Widget Parameters:
output_path    = "/Volumes/workspace/seismo/raw/"
fetch_type     = "FULL_LOAD"              # FULL_LOAD / SAMPLE / INCREMENTAL_LOAD
start_year     = "2019"
end_year       = "2024"
days_back      = "7"                      # For incremental loads
min_magnitude  = "2.5"
```

**What it does:**
- Connects to USGS API
- Fetches earthquake data based on selected type
- Saves GeoJSON directly to DBFS
- Validates file creation

**Output:**
- Full Load: `usgs_earthquake_full_load_2019_2024.json` (~200 MB, ~100k events)
- Sample: `usgs_earthquake_sample.json` (~15 MB, ~10k events)
- Incremental: `usgs_earthquake_incremental_YYYYMMDD_HHMMSS.json` (~1 MB)

---

### **Scenario 1: Standard Full Load (Initial Baseline)**

Load historical 6-year dataset into Bronze and Silver.

#### **Step 1: Bronze Ingestion**
```python
# Notebook: 01_Bronze_Ingestion.py
# Widget Parameters:
source_path    = "/Volumes/workspace/seismo/raw/usgs_earthquake_full_load_2019_2024.json"
ingestion_type = "FULL_LOAD"
batch_id       = "FULL_2026-10-09"        # Or leave blank for auto-generation
run_date       = "2026-10-09"             # Or leave blank for today
force          = "false"
database       = "seismo"
```

**What Happens:**
1. Reads GeoJSON with explicit `ENVELOPE_SCHEMA` (no inferSchema)
2. Explodes features array and extracts event_id
3. Calculates `record_hash = MD5(raw_json)` for change detection
4. De-duplicates within batch (keeps most recent `updated_ms`)
5. Adds Bronze metadata with single `load_timestamp` for entire batch
6. Appends to Bronze table with `is_current = TRUE`
7. Logs execution to `pipeline_execution_log`

**Expected Results:**
- Bronze table populated with ~100,000+ events
- All records have `is_current = TRUE`
- `pipeline_execution_log` shows SUCCESS
- `file_operation_log` shows READ and UPDATE operations

#### **Step 2: Silver Transformation**
```python
# Notebook: 02_Silver_transformations.py
# Widget Parameters:
batch_id  = "FULL_2026-10-09"    # Must match Bronze batch_id
force     = "false"
database  = "seismo"
```

**What Happens:**
1. Reads current Bronze records (`is_current = TRUE`) for batch
2. Parses JSON using explicit `FEATURE_SCHEMA`
3. Detects schema drift (unexpected top-level keys) → quarantines
4. Extracts and casts fields (epoch ms → timestamp, strings → doubles)
5. Validates data bounds (lat/lon, magnitude limits) → quarantines invalid
6. Enriches with derived columns (country, region, categories)
7. De-duplicates by event_id (keeps highest `event_updated_time`)
8. MERGE INTO Silver (upsert on event_id)
9. Logs execution and file operations

**Expected Results:**
- Silver table populated with typed, cleansed data
- All timestamps converted from epoch milliseconds
- Derived columns populated (country, region, depth_category, magnitude_category)
- Invalid records in `silver_seismic_events_quarantine`
- Schema drift logged to `error_log`

---

### **Scenario 2: Incremental Load (Updates & New Events)**

Process new events and magnitude updates from USGS.

#### **Step 1: Fetch Incremental Data**
```python
# Notebook: 00_Fetch_USGS_Data.py
# Widget Parameters:
output_path    = "/Volumes/workspace/seismo/raw/"
fetch_type     = "INCREMENTAL_LOAD"
days_back      = "7"
min_magnitude  = "2.5"
```

This creates a timestamped file with recent updates.

#### **Step 2: Bronze Ingestion**
```python
# Notebook: 01_Bronze_Ingestion.py
# Widget Parameters:
source_path    = "/Volumes/workspace/seismo/raw/usgs_earthquake_incremental_20261010_143022.json"
ingestion_type = "INCREMENTAL_LOAD"
batch_id       = "INCR_2026-10-10"
run_date       = "2026-10-10"
force          = "false"
```

**What Happens (SCD Type 2 Logic):**
1. Reads new/updated events
2. Compares `record_hash` with existing current records
3. **If hash unchanged:** Skip (no-op)
4. **If hash changed (magnitude updated, status changed, etc.):**
   - Old version: Set `is_current = FALSE`, `valid_to = new_load_timestamp`
   - New version: Insert with `is_current = TRUE`, `valid_from = new_load_timestamp`
5. **If new event:** Insert with `is_current = TRUE`

**Result:** Complete audit trail of all changes to each event.

#### **Step 3: Silver Transformation**
```python
# Notebook: 02_Silver_transformations.py
# Widget Parameters:
batch_id  = "INCR_2026-10-10"
force     = "false"
```

**What Happens:**
- MERGE updates existing events only if `event_updated_time` is newer
- New events inserted
- `modification_sequence` incremented for updated records
- Old data never overwrites newer updates (timestamp-based guard)

---

### **Scenario 3: Backfill (Historical Re-processing)**

Re-run a past batch (e.g., after fixing data quality issues or schema changes).

```python
# Bronze Backfill
# Notebook: 01_Bronze_Ingestion.py
source_path    = "/Volumes/workspace/seismo/raw/historical/september_data.json"
ingestion_type = "BACKFILL"
batch_id       = "BACKFILL_2026-09-01"
run_date       = "2026-09-01"
force          = "false"                  # Use "true" if batch already succeeded

# Silver Backfill
# Notebook: 02_Silver_transformations.py
batch_id  = "BACKFILL_2026-09-01"
force     = "true"                        # Required if batch already processed
```

**Use Cases:**
- Recovering from data loss
- Reprocessing after schema evolution
- Loading archived data from new source

---

### **Scenario 4: Re-run After Failure**

If a pipeline run fails, simply re-run with the same `batch_id`.

```python
batch_id = "INCR_2026-10-10"    # Same batch_id that failed
force    = "false"               # Not needed for failed runs
```

**Behavior:**
- Failed runs are NOT blocked by idempotency checks
- Only SUCCESS/PARTIAL runs require `force=true` to re-run
- Check `pipeline_execution_log` for failure details

---

### **Scenario 5: Force Re-run (Idempotency Override)**

Re-process a batch that already succeeded (e.g., after pipeline logic changes).

```python
batch_id = "FULL_2026-10-09"    # Already succeeded batch
force    = "true"                # REQUIRED to override idempotency
```

**Warning:** Even with `force=true`, data-level idempotency prevents duplicates:
- Bronze: Unchanged hashes → 0 rows written
- Silver: Same `event_updated_time` → 0 rows updated

---

### **Verification Queries**

#### **Check Execution Status**
```sql
SELECT execution_id, batch_id, layer, load_type, status, 
       records_processed, records_inserted, records_updated, execution_duration_sec
FROM pipeline_execution_log
ORDER BY execution_id DESC
LIMIT 10;
```

#### **Verify Bronze SCD2 (Version History)**
```sql
-- Show all versions of events that changed
SELECT event_id, batch_id, is_current, valid_from, valid_to,
       LEFT(raw_json, 100) AS json_preview
FROM bronze_seismic_events
WHERE event_id IN (
    SELECT event_id FROM bronze_seismic_events 
    GROUP BY event_id HAVING COUNT(*) > 1
)
ORDER BY event_id, valid_from;
```

#### **Verify Silver Idempotency (No Duplicates)**
```sql
-- Must return 0 rows
SELECT event_id, COUNT(*) AS duplicate_count
FROM silver_seismic_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

#### **Check File Operations**
```sql
SELECT operation_timestamp, layer, operation_type, file_name, 
       batch_id, records_affected, operation_status
FROM file_operation_log
ORDER BY operation_timestamp DESC
LIMIT 20;
```

#### **Review Quarantined Records**
```sql
SELECT quarantine_id, event_id, batch_id, quarantine_reason, 
       failed_columns, reprocessing_status
FROM silver_seismic_events_quarantine
ORDER BY quarantine_timestamp DESC;
```

#### **Check for Errors**
```sql
SELECT error_timestamp, layer, error_type, error_message, 
       resolution_status
FROM error_log
WHERE resolution_status = 'PENDING'
ORDER BY error_timestamp DESC;
```

#### **Point-in-Time Query (Bronze Historical State)**
```sql
-- State of the world on Oct 1, 2026
SELECT event_id, 
       GET_JSON_OBJECT(raw_json,'$.properties.mag') AS magnitude, 
       valid_from, valid_to
FROM bronze_seismic_events
WHERE valid_from <= TIMESTAMP '2026-10-01 23:59:59'
  AND (valid_to > TIMESTAMP '2026-10-01 23:59:59' OR valid_to IS NULL)
LIMIT 20;
```

---

## 🏗️ Architecture Diagram

### Medallion Data Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  DATABRICKS WORKSPACE                                        │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  00_Fetch_USGS_Data.py                                 │ │
│  │  • Connect to USGS API                                 │ │
│  │  • Fetch GeoJSON data                                  │ │
│  │  • Save to DBFS                                        │ │
│  └──────────────────────┬─────────────────────────────────┘ │
│                         │                                   │
│                         ▼                                   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  DBFS: /Volumes/workspace/seismo/raw/                  │ │
│  │  • usgs_earthquake_full_load_2019_2024.json            │ │
│  │  • usgs_earthquake_incremental_*.json                  │ │
│  └──────────────────────┬─────────────────────────────────┘ │
│                         │                                   │
│                         ▼                                   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  01_Bronze_Ingestion.py (RAW → BRONZE)                 │ │
│  │  • Explicit schema enforcement (ENVELOPE_SCHEMA)       │ │
│  │  • MD5 hash-based change detection                     │ │
│  │  • SCD Type 2 versioning                               │ │
│  │  • Idempotent execution                                │ │
│  └──────────────────────┬─────────────────────────────────┘ │
│                         │                                   │
│                         ▼                                   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  BRONZE LAYER (Delta Lake)                             │ │
│  │  • Table: bronze_seismic_events                        │ │
│  │  • SCD Type 2: Complete version history                │ │
│  │  • Partitioned: (ingestion_type, year, month)          │ │
│  └──────────────────────┬─────────────────────────────────┘ │
│                         │                                   │
│                         ▼                                   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  02_Silver_transformations.py (BRONZE → SILVER)        │ │
│  │  • Parse JSON with explicit FEATURE_SCHEMA             │ │
│  │  • Schema drift detection & quarantine                 │ │
│  │  • Type casting & validation                           │ │
│  │  • Enrichment (country, region, categories)            │ │
│  │  • MERGE INTO (idempotent upsert)                      │ │
│  └──────────────────────┬─────────────────────────────────┘ │
│                         │                                   │
│                         ▼                                   │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  SILVER LAYER (Delta Lake)                             │ │
│  │  • Table: silver_seismic_events                        │ │
│  │  • SCD Type 1: Latest version only                     │ │
│  │  • Partitioned: (year, month) from event_time          │ │
│  │  • Quarantine: silver_seismic_events_quarantine        │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  OPERATIONAL TABLES (Audit & Logging)                  │ │
│  │  • pipeline_execution_log                              │ │
│  │  • file_operation_log                                  │ │
│  │  • error_log                                           │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
seismo/
├── .gitignore                              # Git ignore rules
├── README.md                               # This file (Phase 2 documentation)
│
├── data/
│   └── samples/                            # Sample data files
│       ├── usgs_earthquake_sample.json     # 1-month sample (~15 MB)
│       └── usgs_earthquake_incremental.json # 7-day updates (~1 MB)
│
├── docs/                                   # Project documentation
│   ├── Phase1_Proposal.md                  # Phase 1 project proposal
│   ├── Phase2_Design.md                    # Phase 2 design decisions
│   ├── Phase2_requirements.md              # Course requirements
│   ├── Phase2_Req_new.md                   # Updated requirements
│   └── Phase_2_Rubric.md                   # Grading rubric
│
├── notebooks/                              # Databricks PySpark notebooks
│   ├── config/                             # Shared configuration modules
│   │   ├── schemas.py                      # All PySpark schema definitions
│   │   ├── constants.py                    # Pipeline constants and table names
│   │   └── utils.py                        # Utility functions
│   │
│   ├── 00_Setup.py                         # Initial table creation
│   ├── 00_Fetch_USGS_Data.py               # API data ingestion to DBFS
│   ├── 01_Bronze_Ingestion.py              # RAW → BRONZE layer
│   ├── 02_Silver_transformations.py        # BRONZE → SILVER layer
│   └── Table_Row_Counts.py                 # Data verification utility
│
└── src/                                    # Source code (not part of Databricks pipeline)
    └── ingestion/                          # Local ingestion scripts (gitignored)
        ├── fetch_usgs_data.py              # Local data fetcher (not used in Databricks)
        └── requirements.txt                # Python dependencies
```

**Note:** The full load file `usgs_earthquake_full_load_2019_2024.json` (~200 MB) is excluded from git and uploaded directly to Databricks DBFS.

---

## 🔧 Technical Implementation Details

### **1. Strict Schema Enforcement**

**Requirement:** No `inferSchema=True` anywhere in the pipeline

**Implementation:**
```python
# config/schemas.py defines all schemas explicitly

# Bronze: Read GeoJSON with defined envelope schema
ENVELOPE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("metadata", StructType([...]), True),
    StructField("features", ArrayType(StringType()), True),  # Raw JSON strings
])

raw_df = spark.read.schema(ENVELOPE_SCHEMA).json(source_path)

# Silver: Parse individual features with defined schema
FEATURE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("properties", StructType([...])),
    StructField("geometry", StructType([...])),
])

parsed = df.withColumn("parsed", F.from_json("raw_json", FEATURE_SCHEMA))
```

---

### **2. Idempotency Implementation**

**Bronze Layer:**
```python
# Hash-based change detection
bronze_df = bronze_df.withColumn("record_hash", F.md5("raw_json"))

# Compare with existing current records
existing_df = spark.table("bronze_seismic_events").filter("is_current = TRUE")
changed = bronze_df.join(existing_df, 
    (bronze_df.event_id == existing_df.event_id) & 
    (bronze_df.record_hash != existing_df.record_hash),
    "left_anti")

# Only events with different hash proceed to MERGE
# Unchanged events → 0 rows written
```

**Silver Layer:**
```python
# MERGE with timestamp-based guard
delta_table.merge(source, "target.event_id = source.event_id")
  .whenMatchedUpdate(
    condition="source.event_updated_time > target.event_updated_time",
    set={...}  # Only update if source is newer
  )
  .whenNotMatchedInsert(values={...})
  .execute()

# Re-running same batch → 0 rows updated (timestamps unchanged)
```

---

### **3. SCD Type 2 Pattern (Bronze)**

**Single MERGE Implementation:**
```python
# For changed events, create two copies in source:
# Copy A: merge_key = event_id → matches current row → expires it
# Copy B: merge_key = NULL → never matches → inserts new version

expire = changed.filter("had_current").withColumn("merge_key", F.col("event_id"))
insert = changed.withColumn("merge_key", F.lit(None).cast("string"))
src = expire.unionByName(insert)

DeltaTable.forName(spark, "bronze_seismic_events")
  .merge(src, "t.event_id = s.merge_key AND t.is_current = TRUE")
  .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s.valid_from"})
  .whenNotMatchedInsert(condition="s.merge_key IS NULL", values={...})
  .execute()
```

**Result:** All versions preserved, single load_timestamp per batch, full audit trail.

---

### **4. Schema Drift Handling**

```python
# Expected top-level keys defined in schemas.py
EXPECTED_TOP_KEYS = ["type", "id", "properties", "geometry"]

# Per-record drift detection
parsed = parsed.withColumn(
    "_unexpected_keys",
    F.array_except(F.json_object_keys("raw_json"), expected_keys_lit)
).withColumn(
    "_has_schema_drift",
    F.size("_unexpected_keys") > 0
)

# Log drift to error_log
for key in unexpected_keys:
    log_error(error_type="SCHEMA_DRIFT", 
              message=f"Unexpected key: {key}")

# Quarantine records with schema drift
drift_records.write.mode("append").saveAsTable("silver_seismic_events_quarantine")

# Continue processing conforming records
parsed = parsed.filter(~F.col("_has_schema_drift"))
```

---

### **5. Batch-Level Load Timestamp**

**Decision:** All records in a batch share the same `load_timestamp`

**Implementation:**
```python
def run_bronze(source_path, batch_id, ingestion_type):
    start = utc_now()
    load_ts = start  # Captured ONCE at pipeline start
    
    # All literals use the same load_ts
    staged = good.select(
        "event_id", "raw_json",
        F.lit(batch_id).alias("batch_id"),
        F.lit(load_ts).alias("valid_from"),      # Same for all rows
        F.lit(load_ts).alias("created_timestamp"),
        F.lit(load_ts.year).alias("year"),
        F.lit(load_ts.month).alias("month"),
    )
    
    # Guarantees identical timestamp across entire batch
```

**Benefit:** Atomic batch semantics, accurate point-in-time queries.

---

### **6. Comprehensive Audit Framework**

**Three-tier logging:**

1. **Execution Log** (batch-level)
   - Every pipeline run logged (success or failure)
   - Metrics: records processed, inserted, updated, errored
   - Duration tracking

2. **File Operation Log** (file-level)
   - All CREATE/READ/UPDATE/DELETE operations
   - Links operations to batch_id
   - Tracks which files were processed when

3. **Error Log** (record-level)
   - Individual error details
   - Stack traces for debugging
   - Resolution tracking workflow

**Example:**
```python
def write_exec_log(start, end, batch_id, status, metrics, error_msg):
    row = (start, end, "01_Bronze_Ingestion", batch_id, "RAW_TO_BRONZE",
           "FULL_LOAD", source_path, status,
           metrics["total"], metrics["inserted"], metrics["updated"],
           metrics["errored"], error_msg,
           int((end - start).total_seconds()), utc_now())
    spark.createDataFrame([row], EXECUTION_LOG_SCHEMA) \
         .write.mode("append").saveAsTable("pipeline_execution_log")
```

---

## 🧪 Testing & Validation

### **Idempotency Tests**

**Test 1: Re-run same full load**
```sql
-- First run: FULL_2026-10-09
-- records_inserted = 100,000

-- Second run: FULL_2026-10-09 with force=true
-- records_inserted = 0 (all hashes unchanged)
-- records_updated = 0 (no versions expired)

SELECT batch_id, status, records_inserted, records_updated
FROM pipeline_execution_log
WHERE batch_id = 'FULL_2026-10-09'
ORDER BY execution_id;
```

**Test 2: Incremental with no changes**
```sql
-- Run incremental load with unchanged events
-- records_processed = 1,000
-- records_inserted = 0
-- records_updated = 0
```

---

### **SCD Type 2 Tests**

**Test 3: Event magnitude update**
```sql
-- Initial: us6000abc1, magnitude=4.5, is_current=TRUE
-- Update:  us6000abc1, magnitude=4.7, is_current=TRUE

-- Query: Show version history
SELECT event_id, 
       GET_JSON_OBJECT(raw_json,'$.properties.mag') AS magnitude,
       is_current, valid_from, valid_to
FROM bronze_seismic_events
WHERE event_id = 'us6000abc1'
ORDER BY valid_from;

-- Expected:
-- Row 1: mag=4.5, is_current=FALSE, valid_to=2026-10-10 00:00:00
-- Row 2: mag=4.7, is_current=TRUE,  valid_to=NULL
```

---

### **Data Quality Tests**

**Test 4: Invalid records quarantined**
```sql
-- Events with lat > 90 or lon > 180 or magnitude > 10
SELECT COUNT(*) FROM silver_seismic_events_quarantine
WHERE quarantine_reason = 'Validation bounds or required null check failed';

-- Expected: > 0 (some bad data exists in real-world datasets)
```

**Test 5: No duplicates in Silver**
```sql
-- Must return 0 rows
SELECT event_id, COUNT(*) 
FROM silver_seismic_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

---

### **Schema Drift Tests**

**Test 6: Unexpected JSON key**
```sql
-- If USGS adds new field "seismic_intensity_v2"
SELECT error_type, error_message, COUNT(*) 
FROM error_log
WHERE error_type = 'SCHEMA_DRIFT'
GROUP BY error_type, error_message;

-- Expected: Log entry for unexpected key
```

---

## 📊 Phase 2 Achievements

### **Requirements Met**

✅ **Infrastructure & Environment**
- Databricks workspace configured
- All code committed to GitHub
- DBFS volumes configured for data storage

✅ **Data Modeling & Schema Enforcement**
- Complete data dictionaries for Bronze and Silver (documented above)
- Strict schema enforcement using PySpark StructType (no inferSchema)
- Explicit type casting for all fields (epoch ms → timestamp, strings → doubles)
- Metadata integration: `load_timestamp`, `created_timestamp`, `updated_timestamp` on all records

✅ **Pipeline Robustness & Idempotency**
- Idempotent execution proven (re-running same data → 0 inserts/updates)
- MERGE INTO patterns for both Bronze (SCD2) and Silver (SCD1)
- Parameterized design: batch_id, run_date, force flags support backfills
- Schema drift handling: detect → log → quarantine (pipeline never crashes)

✅ **Audit & Execution Logging**
- Three dedicated logging tables: `pipeline_execution_log`, `file_operation_log`, `error_log`
- Every run logged with: layer, source, start/end times, status, row counts
- Both full and incremental loads fully audited

---

### **Key Design Decisions**

1. **Bronze Schema Approach:** Explicit `ENVELOPE_SCHEMA` with raw_json as STRING (preserves all fields)
2. **Silver Schema Drift:** Dynamic column addition disabled, drift logged and quarantined
3. **Type Casting Errors:** Failed records quarantined to `silver_seismic_events_quarantine`
4. **Batch ID Format:** `{PREFIX}_{YYYY-MM-DD}` (e.g., FULL_2026-10-09)
5. **Load Timestamp:** Single timestamp captured at pipeline start, used for entire batch
6. **Idempotency Strategy:** Check execution log + hash-based change detection
7. **Deleted Events:** Soft delete with `is_active = FALSE` flag

---

## 🎓 Course Compliance

**DS-3001 Data Engineering - Phase 2 Rubric:**

| Requirement | Status | Evidence |
|-------------|--------|----------|
| **Databricks workspace setup** | ✅ | All notebooks run in Databricks Community Edition |
| **GitHub version control** | ✅ | All code committed with meaningful history |
| **Bronze & Silver data models documented** | ✅ | Complete schemas with data types above |
| **No inferSchema usage** | ✅ | All schemas explicitly defined in `config/schemas.py` |
| **Strict type casting** | ✅ | Epoch ms → timestamp, all fields explicitly cast |
| **Metadata columns (load_timestamp)** | ✅ | All tables include load_timestamp, created_timestamp |
| **Idempotent execution** | ✅ | Re-runs produce 0 inserts/updates (hash-based) |
| **MERGE INTO pattern** | ✅ | Both Bronze (SCD2) and Silver (SCD1) use MERGE |
| **Parameterized backfills** | ✅ | batch_id, run_date, force parameters support reruns |
| **Schema drift handling** | ✅ | Detection → logging → quarantine (no crashes) |
| **Execution logging** | ✅ | pipeline_execution_log tracks all runs |
| **Audit metrics** | ✅ | Layer, source, times, status, row counts logged |
| **README with execution guide** | ✅ | This document provides complete instructions |

---

## 📚 References

- [USGS FDSN Event Web Service API](https://earthquake.usgs.gov/fdsnws/event/1/)
- [Databricks Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
- [PySpark SQL Functions Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Slowly Changing Dimensions (SCD Type 2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension)

---

## 👨‍💻 Author

**FAST-NUCES Student**  
DS-3001 Data Engineering Project  
Phase 2: Core Pipeline Engineering  
Submission Date: October 10, 2026

---

## 📄 License

This project is part of an academic assignment for DS-3001 at FAST-NUCES.
