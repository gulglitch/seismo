# 🌍 Automated USGS Seismic Event Telemetry Pipeline

**DS-3001 Data Engineering Project | FAST-NUCES**

> Global earthquake monitoring and real-time seismic event tracking using Databricks Medallion Architecture

---

## 📋 Project Overview

This project builds an **automated data engineering pipeline** for global seismic event monitoring, tracking earthquake occurrences, magnitude distributions, focal depths, and geographic patterns. The pipeline ingests data from the USGS (United States Geological Survey) and processes it through a **Bronze → Silver → Gold** Medallion architecture to provide refined operational data for disaster risk analysis and geological research.

### Key Features

- ✅ **Real-time seismic event tracking** from USGS FDSN API
- ✅ **Medallion Architecture** (Bronze, Silver, Gold layers)
- ✅ **Full & Incremental Load** patterns
- ✅ **Delta Lake** for ACID transactions and time travel
- ✅ **PySpark** data transformations in Databricks
- ✅ **Business Intelligence dashboards** for geological insights

---

## 🎯 Project Goals

1. **Automate data ingestion** from USGS earthquake API
2. **Transform raw GeoJSON** into structured analytical tables
3. **Support disaster risk analysis** with clean, queryable data
4. **Enable BI dashboards** for seismic trend visualization
5. **Handle updates and deletions** through incremental processing

---

## 📊 Data Source

**API:** [USGS FDSN Event Web Service](https://earthquake.usgs.gov/fdsnws/event/1/query)

- **Format:** GeoJSON
- **Authentication:** None (public API)
- **Rate Limits:** Reasonable limits for academic use
- **Data Quality:** Highly reliable, maintained by U.S. government

### Data Files Generated

The ingestion script generates three distinct files optimized for different purposes:

1. **Full Load** (`usgs_earthquake_full_load.json`)
   - **Size:** ~200 MB
   - **Period:** 6 years (2019-2024)
   - **Magnitude Filter:** ≥2.5
   - **Purpose:** Historical baseline for Databricks upload
   - **Event Count:** ~100,000+ events

2. **Sample Load** (`usgs_earthquake_sample.json`)
   - **Size:** ~15 MB
   - **Period:** 1 month (December 2024)
   - **Magnitude Filter:** ≥2.5
   - **Purpose:** GitHub repository sample
   - **Event Count:** ~10,000+ events

3. **Incremental Load** (`usgs_earthquake_incremental.json`)
   - **Size:** ~1 MB
   - **Period:** Last 7 days (rolling)
   - **Magnitude Filter:** ≥2.5 (matches full load)
   - **Purpose:** Daily updates with new events, updates, and deletions
   - **Event Count:** ~1,000+ events
   - **Special:** Includes deleted events for soft-delete logic

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
    "tz": null,
    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000m2x2",
    "status": "reviewed",
    "felt": null,
    "cdi": null,
    "mmi": null,
    "alert": null,
    "sig": 312,
    "magType": "mb",
    "type": "earthquake"
  },
  "geometry": {
    "type": "Point",
    "coordinates": [143.3492, 12.2725, 30.12]
  }
}
```

---

## 🗄️ **Data Models**

### **Bronze Layer: `bronze_seismic_events`**

**Purpose:** Raw data landing zone with SCD Type 2 (full historical tracking)

**Primary Key:** `(event_id, batch_id)` — Composite key ensures one version per event per batch

**Partitioning:** `PARTITIONED BY (ingestion_type, year, month)`

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `bronze_id` | BIGINT | No | Auto-increment identity (system-generated) |
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
| `year` | INT | Yes | Partition column (from created_timestamp) |
| `month` | INT | Yes | Partition column (from created_timestamp) |

**Key Features:**
- ✅ Explicit schema enforcement via `ENVELOPE_SCHEMA` (no `inferSchema`)
- ✅ SCD Type 2: Maintains complete version history of every event
- ✅ Idempotent: Re-running same data inserts 0 rows (hash-based deduplication)
- ✅ Change detection: Only events with different `record_hash` create new versions
- ✅ Point-in-time queries: Reconstruct data state at any past timestamp

---

### **Silver Layer: `silver_seismic_events`**

**Purpose:** Cleansed, typed, analytics-ready data (SCD Type 1 - latest version only)

**Primary Key:** `event_id` — Unique USGS event identifier

**Partitioning:** `PARTITIONED BY (year, month)` — Based on `event_time` for time-series queries

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `event_id` | STRING | No | USGS event identifier (PK) |
| `magnitude` | DOUBLE | Yes | Earthquake magnitude (can be NULL) |
| `magnitude_type` | STRING | Yes | Magnitude scale (mb, ml, mw, md, etc.) |
| `place` | STRING | Yes | Location description (e.g., "24 km ENE of Beluga, Alaska") |
| `longitude` | DOUBLE | Yes | Geographic longitude (-180 to 180) |
| `latitude` | DOUBLE | Yes | Geographic latitude (-90 to 90) |
| `depth_km` | DOUBLE | Yes | Focal depth in kilometers (negative = above sea level) |
| `event_time` | TIMESTAMP | Yes | UTC timestamp of earthquake occurrence |
| `event_updated_time` | TIMESTAMP | Yes | USGS last revision timestamp (drives MERGE logic) |
| `status` | STRING | Yes | Review status (automatic, reviewed, deleted) |
| `felt_reports` | INT | Yes | Number of "Did You Feel It?" reports |
| `cdi` | DOUBLE | Yes | Community Decimal Intensity |
| `mmi` | DOUBLE | Yes | Modified Mercalli Intensity |
| `alert_level` | STRING | Yes | PAGER alert level (green, yellow, orange, red) |
| `significance` | INT | Yes | Event significance score (USGS calculated) |
| `tsunami_flag` | BOOLEAN | Yes | Whether event is in tsunami-capable region |
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

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `quarantine_id` | BIGINT | No | Auto-increment identity |
| `event_id` | STRING | Yes | USGS event ID (if parseable) |
| `batch_id` | STRING | Yes | Source batch identifier |
| `quarantine_reason` | STRING | No | Why record was quarantined |
| `failed_columns` | STRING | Yes | Which columns failed validation |
| `raw_json` | STRING | Yes | Original JSON for reprocessing |
| `error_details` | STRING | Yes | Detailed error information |
| `quarantine_timestamp` | TIMESTAMP | No | When quarantined |
| `reprocessing_status` | STRING | No | PENDING / REPROCESSED / DISCARDED |
| `created_timestamp` | TIMESTAMP | No | Record creation time |
| `updated_timestamp` | TIMESTAMP | Yes | Record modification time |

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
| `layer` | STRING | STAGING / BRONZE / SILVER |
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
| `layer` | STRING | STAGING / BRONZE / SILVER |
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

## 🚀 **Execution Guide**

### **Prerequisites**

1. **Upload data files to DBFS:**
   ```bash
   # Upload to Databricks File System
   /Volumes/workspace/seismo/raw/usgs_earthquake_full_load.json       # ~200 MB
   /Volumes/workspace/seismo/raw/usgs_earthquake_incremental.json     # ~1 MB
   /Volumes/workspace/seismo/raw/usgs_earthquake_sample.json          # ~15 MB
   ```

2. **Run setup notebook once:**
   ```
   00_Setup.py
   ```
   This creates all tables and configures the database.

---

### **Scenario 1: Standard Full Load (Initial Baseline)**

Load historical 6-year dataset into Bronze and Silver.

#### **Step 1: Bronze Ingestion**
```python
# Notebook: 01_Bronze_Ingestion.py
# Widget Parameters:
source_path    = "/Volumes/workspace/seismo/raw/usgs_earthquake_full_load.json"
ingestion_type = "FULL_LOAD"
batch_id       = "FULL_2026-10-09"        # Or leave blank for auto-generation
run_date       = "2026-10-09"             # Or leave blank for today
force          = "false"
database       = "seismo"
```

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

**Expected Results:**
- Silver table populated with typed, cleansed data
- All timestamps converted from epoch milliseconds
- Derived columns populated (country, region, categories)
- `file_operation_log` shows READ (Bronze) and UPDATE (Silver) operations

---

### **Scenario 2: Standard Incremental Load (Daily Updates)**

Process new events and updates from USGS.

#### **Step 1: Bronze Ingestion**
```python
# Notebook: 01_Bronze_Ingestion.py
# Widget Parameters:
source_path    = "/Volumes/workspace/seismo/raw/usgs_earthquake_incremental.json"
ingestion_type = "INCREMENTAL_LOAD"
batch_id       = "INCR_2026-10-10"        # Or blank for auto: INCR_{today}
run_date       = "2026-10-10"
force          = "false"
database       = "seismo"
```

**What Happens:**
- New events → inserted with `is_current = TRUE`
- Updated events → old version gets `is_current = FALSE`, `valid_to = new_load_timestamp`
- New version inserted with `is_current = TRUE`, `valid_from = new_load_timestamp`
- Unchanged events → skipped (hash comparison)

#### **Step 2: Silver Transformation**
```python
# Notebook: 02_Silver_transformations.py
# Widget Parameters:
batch_id  = "INCR_2026-10-10"
force     = "false"
database  = "seismo"
```

**What Happens:**
- MERGE updates existing events only if `event_updated_time` is newer
- New events inserted
- `modification_sequence` incremented for updated records

---

### **Scenario 3: Backfill (Historical Re-processing)**

Re-run a past batch (e.g., after fixing data quality issues or schema changes).

#### **Bronze Backfill**
```python
# Notebook: 01_Bronze_Ingestion.py
# Widget Parameters:
source_path    = "/Volumes/workspace/seismo/raw/historical/september_data.json"
ingestion_type = "BACKFILL"
batch_id       = "BACKFILL_2026-09-01"
run_date       = "2026-09-01"
force          = "false"                  # Use "true" if batch already succeeded
database       = "seismo"
```

**Use Cases:**
- Recovering from data loss
- Reprocessing after schema evolution
- Loading archived data from new source

#### **Silver Backfill**
```python
# Notebook: 02_Silver_transformations.py
# Widget Parameters:
batch_id  = "BACKFILL_2026-09-01"
force     = "true"                        # Required if batch already processed
database  = "seismo"
```

**Note:** Silver can re-process any historical Bronze batch. The MERGE logic prevents old data from overwriting newer updates via `event_updated_time` comparison.

---

### **Scenario 4: Re-run After Failure**

If a pipeline run fails, simply re-run with the same `batch_id`.

```python
# Notebook: 01_Bronze_Ingestion.py or 02_Silver_transformations.py
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
# Any notebook
batch_id = "FULL_2026-10-09"    # Already succeeded batch
force    = "true"                # REQUIRED to override idempotency
```

**Warning:** Use cautiously. Even with `force=true`, data-level idempotency prevents duplicates:
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

---

## 🏗️ **Architecture Diagram**

### Medallion Data Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  LOCAL / GITHUB ACTIONS                                      │
│  ┌────────────────────────────────────┐                     │
│  │  Python Ingestion Script           │                     │
│  │  - Fetch from USGS API             │                     │
│  │  - Save GeoJSON files              │                     │
│  └────────────────┬───────────────────┘                     │
└───────────────────┼──────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  DATABRICKS WORKSPACE (Phase 2 & 3)                         │
│                                                             │
│  ┌────────────────────────────────────┐                     │
│  │  BRONZE LAYER (Raw)                │                     │
│  │  - Raw GeoJSON storage             │                     │
│  │  - Append-only Delta Lake          │                     │
│  │  - Ingestion metadata              │                     │
│  └────────────────┬───────────────────┘                     │
│                   │                                         │
│                   ▼                                         │
│  ┌────────────────────────────────────┐                     │
│  │  SILVER LAYER (Cleansed)           │                     │
│  │  - Flatten nested JSON             │                     │
│  │  - Type conversions                │                     │
│  │  - Handle updates/deletes          │                     │
│  │  - Standardized schema             │                     │
│  └────────────────┬───────────────────┘                     │
│                   │                                         │
│                   ▼                                         │
│  ┌────────────────────────────────────┐                     │
│  │  GOLD LAYER (Analytics)            │                     │
│  │  - Star schema (Fact + Dims)       │                     │
│  │  - Pre-aggregated KPIs             │                     │
│  │  - Optimized for BI tools          │                     │
│  └────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│  BUSINESS INTELLIGENCE                                      │
│  - Power BI / Tableau dashboards                            │
│  - Geographic heat maps                                     │
│  - Time-series analysis                                     │
└─────────────────────────────────────────────────────────────┘
```

### Data Layer Details

| Layer | Storage | Processing Logic |
|-------|---------|------------------|
| **Bronze** | Delta Lake (Append-Only) | Raw GeoJSON with ingestion metadata (ingested_at, source_file, batch_id) |
| **Silver** | Delta Lake (Standardized) | 1. Flatten nested JSON<br>2. Convert timestamps<br>3. Cast coordinates/magnitude<br>4. MERGE on event_id for updates |
| **Gold** | Delta Lake (Star Schema) | Dimensional tables: FactSeismicEvent, DimLocation, DimMagnitudeClass with pre-aggregated KPIs |

---

## 📁 Repository Structure

```
seismo/
├── data/
│   └── samples/              # Sample data files
│       ├── usgs_earthquake_full_load.json         # 6-year baseline (~200 MB) - Upload to Databricks
│       ├── usgs_earthquake_sample.json            # 1-month sample (~15 MB) - Commit to GitHub
│       └── usgs_earthquake_incremental.json       # 7-day rolling updates (~1 MB) - Daily incremental
├── src/
│   └── ingestion/            # Data ingestion scripts
│       ├── fetch_usgs_data.py                     # Main ingestion script
│       └── requirements.txt                       # Python dependencies
├── notebooks/                # Databricks PySpark notebooks (Phase 2)
│   └── README.md             # Notebook documentation
└── README.md                 # This file
```

---

## 🚀 Phase 1: Setup & Data Acquisition (Current Phase)

### Prerequisites

- Python 3.8+
- Internet connection for API access

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/gulglitch/seismo.git
   cd seismo
   ```

2. **Install dependencies:**
   ```bash
   pip install -r src/ingestion/requirements.txt
   ```

3. **Run data ingestion:**
   ```bash
   python src/ingestion/fetch_usgs_data.py
   ```

### What Happens in Phase 1

The ingestion script performs three operations:

1. **Full Load:** Fetches 6-year historical earthquake data (2019-2024)
   - **Time period:** 2019-01-01 to 2024-12-31
   - **Minimum magnitude:** 2.5
   - **Output:** `data/samples/usgs_earthquake_full_load.json`
   - **Size:** ~200 MB (~100,000+ events)
   - **Purpose:** Complete historical baseline for Databricks upload

2. **Sample Load:** Fetches 1-month sample for GitHub
   - **Time period:** December 2024
   - **Minimum magnitude:** 2.5
   - **Output:** `data/samples/usgs_earthquake_sample.json`
   - **Size:** ~15 MB (~10,000+ events)
   - **Purpose:** Manageable sample for version control

3. **Incremental Load:** Fetches events updated in the last 7 days
   - **Uses:** `updatedafter` parameter (rolling 7-day window)
   - **Minimum magnitude:** 2.5 (matches full load filter)
   - **Output:** `data/samples/usgs_earthquake_incremental.json`
   - **Size:** ~1 MB (~1,000+ events)
   - **Special:** Includes deleted events (`status='deleted'`) for soft-delete logic
   - **Purpose:** Daily updates to maintain fresh data

### Sample Data Verification

After running the script, you should see output like:

```
============================================================
USGS EARTHQUAKE DATA INGESTION (V2)
DS-3001 Data Engineering Project - Phase 1
============================================================

============================================================
STEP 1: Full Load (for Databricks)
============================================================
FULL LOAD: Multi-Year Historical Data
Period: 2019-2024
Minimum Magnitude: 2.5
============================================================

Batch 1: 2019-01-01 to 2019-02-01... ✓ 8,234 events
Batch 2: 2019-02-01 to 2019-03-01... ✓ 7,891 events
[... processing monthly batches ...]

============================================================
✓ Full Load Complete!
  • Total events: 100,000+
  • File size: ~200 MB
  • Batches processed: 72
  • Saved to: data/samples/usgs_earthquake_full_load.json
============================================================

============================================================
STEP 2: Sample Load (for GitHub)
============================================================
SAMPLE LOAD: For GitHub Repository
Period: 2024-12-01 to 2025-01-01
Minimum Magnitude: 2.5
============================================================

✓ Success!
  • Events fetched: 10,000+
  • File size: ~15 MB
  • Saved to: data/samples/usgs_earthquake_sample.json

============================================================
STEP 3: Incremental Load (for daily updates)
============================================================
INCREMENTAL LOAD: Updates & Deletions
Updated after: [7 days ago]
Days back: 7
Minimum Magnitude: 2.5
Include deleted: Yes
============================================================

✓ Success!
  • Events fetched: 1,000+
  • Deleted events: 5
  • File size: ~1 MB
  • Saved to: data/samples/usgs_earthquake_incremental.json
```

---

## 🔐 Security & Compliance

### PII Assessment

✅ **No Personally Identifiable Information (PII)** exists in this dataset.

The data consists strictly of:
- Geographic coordinates (latitude, longitude, depth)
- Seismic measurements (magnitude, intensity, station counts)
- UTC timestamps
- Event status and metadata

### Data Handling Strategy

- No masking or hashing required
- Location strings (e.g., "12km ENE of Muzaffarabad, Pakistan") will be parsed into standardized country/province codes in the Silver layer
- All data is public and maintained by USGS

---

## 📈 Volume & Frequency Estimates

### Full Load (Historical Baseline)
- **Volume:** ~200 MB
- **Time Period:** 6 years (2019-2024)
- **Minimum Magnitude:** 2.5+
- **Frequency:** One-time historical baseline
- **Event Count:** ~100,000+ seismic events
- **Purpose:** Complete multi-year dataset for Databricks upload

### Sample Load (GitHub)
- **Volume:** ~15 MB
- **Time Period:** 1 month (December 2024)
- **Minimum Magnitude:** 2.5+
- **Frequency:** Generated once for repository
- **Event Count:** ~10,000+ events
- **Purpose:** Manageable sample for version control and testing

### Incremental Load (Daily Updates)
- **Volume:** ~1 MB per load
- **Time Window:** Rolling 7-day window (updated events)
- **Minimum Magnitude:** 2.5+ (matches full load)
- **Frequency:** Daily automated polling
- **Event Count:** ~1,000+ events per load
- **Special Features:**
  - Captures newly registered events
  - Captures magnitude updates (re-evaluated by seismologists)
  - Includes deleted events (`status='deleted'`) for soft-delete logic
- **Annual Growth:** ~365 MB per year (daily incremental)

### Key Changes Based on Instructor Feedback
1. **Unified magnitude filter:** All loads now use 2.5+ (previously incremental used different threshold)
2. **Extended time period:** Full load expanded from 2 years to 6 years
3. **Increased volume:** Full load expanded from ~15 MB to ~200 MB
4. **Longer incremental window:** Changed from 24 hours to 7 days to ensure 1+ MB per file
5. **Three-file structure:** Added dedicated sample file for GitHub while keeping full load for Databricks

### Spark & FinOps Fit
- **In-Memory Processing:** 200 MB full load is highly manageable for PySpark
- **Databricks Compatibility:** Well within Databricks Community Edition limits (15 GB driver memory)
- **Scalability:** Sufficient volume for multi-year trend analysis
- **Cost-Effective:** Incremental loads minimize daily compute requirements

---

## 🎨 Business Intelligence & Dashboards (Phase 3)

### Key Questions Answered

1. Which geographical regions exhibit accelerating seismic activity over time?
2. What is the ratio of shallow focus (<70 km) to deep focus (>300 km) earthquakes across regions?
3. How does seismic energy release correlate with magnitude distributions?

### Planned Visualizations

- **Interactive Geographic Map:** Bubble chart where radius = magnitude, color = depth category
- **Time-Series Frequency Chart:** Monthly event counts + cumulative energy released
- **Magnitude Distribution Histogram:** Breakdown by Richter scale classes (Minor, Moderate, Strong, Major)

---

## ⚙️ FinOps & Infrastructure Management

### Ingestion Decoupling
- **Challenge:** Databricks Free Edition blocks external API calls
- **Solution:** Run Python ingestion script locally or via GitHub Actions
- **Workflow:** 
  1. Fetch data using Python script (generates 3 files)
  2. Upload full load (~200 MB) to DBFS Volumes
  3. Commit sample (~15 MB) and incremental (~1 MB) to GitHub
  4. Process in Databricks from DBFS (not API directly)

### Cluster Optimization
- Auto-termination set to 20 minutes of inactivity
- Delta tables partitioned by year and month
- Optimized scan queries to conserve compute credits

---

## 🗓️ Project Timeline

| Phase | Deliverable | Due Date | Status |
|-------|-------------|----------|--------|
| **Phase 1** | Proposal + GitHub Repo + Sample Data | Sep 26, 2026 | ✅ Complete |
| **Phase 2** | Bronze/Silver/Gold Notebooks | Oct 10, 2026 | 🔄 In Progress |
| **Phase 3** | BI Dashboard + Final Report | Oct 24, 2026 | ⏳ Upcoming |

### Phase 1 Achievements
- ✅ Three distinct data files generated (200 MB full, 15 MB sample, 1 MB incremental)
- ✅ 6-year historical baseline (2019-2024)
- ✅ Unified magnitude filter (2.5+) across all load types
- ✅ Soft-delete support with `includedeleted=true`
- ✅ Monthly batching for large dataset retrieval
- ✅ Comprehensive documentation and proposal

---

## 👥 Team

**Course:** DS-3001 Data Analysis and Visualization  
**Institution:** FAST-NUCES  
**Project:** Automated USGS Seismic Event Telemetry Pipeline

---

## 📚 References

- [USGS Earthquake API Documentation](https://earthquake.usgs.gov/fdsnws/event/1/)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

---

## 📝 License

This is an academic project for DS-3001 Data Engineering course at FAST-NUCES.

---

## 🤝 Contributing

This is a closed academic project. For questions or collaboration, contact the project team through FAST-NUCES DS-3001 course channels.

---

**Last Updated:** October 3, 2026  
**Phase:** Phase 1 Complete | Phase 2 In Progress
