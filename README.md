# Seismo — Real-time Earthquake Data Pipeline

End-to-end Medallion Architecture pipeline that ingests global seismic data from the USGS API, maintains complete historical versions, and delivers business-ready analytics through a star schema optimized for Power BI.

**Built with:** PySpark · Delta Lake · Databricks · Power BI

---

## Architecture Overview

A production-grade lakehouse implementation using the Bronze-Silver-Gold medallion pattern:

- **Bronze Layer:** Raw GeoJSON archive with SCD Type 2 versioning — captures every update to earthquake magnitude, status, and metadata
- **Silver Layer:** Cleansed, typed, analytics-ready events with data quality quarantine and schema drift detection
- **Gold Layer:** Star schema (2 facts, 4 dimensions) with liquid clustering for fast BI queries

### Key Engineering Features

- **Strict schema enforcement** — no `inferSchema` anywhere; all schemas explicitly defined in PySpark StructTypes
- **Complete idempotency** — hash-based change detection ensures re-running the same data writes 0 rows
- **SCD Type 2 history** — Bronze layer maintains full audit trail of every event modification with temporal validity windows
- **Incremental processing** — watermark-based MERGE operations process only changed Silver rows when refreshing Gold
- **Comprehensive audit framework** — execution logs, file operation logs, error tracking, and quarantine tables
- **Automated orchestration** — file-arrival triggers run the complete pipeline end-to-end on new data

---

## Data Flow

```
USGS API (GeoJSON)
    ↓
Bronze Layer (bronze_seismic_events)
    • Raw JSON preservation
    • SCD Type 2 versioning
    • Hash-based change detection
    • Partitioned by ingestion_type/year/month
    ↓
Silver Layer (silver_seismic_events)
    • Type enforcement (epoch ms → timestamps)
    • Data quality validation → quarantine
    • Schema drift detection → error_log
    • Enrichment (country/region parsing, categories)
    • SCD Type 1 (latest version only)
    ↓
Gold Layer (star schema)
    • Fact: gold_fact_seismic_event (~166K events)
    • Fact: gold_fact_daily_location_summary (aggregated)
    • Dimensions: date, location, magnitude_class, depth_class
    • Liquid clustering on (date_key, location_key)
    • Incremental MERGE via watermark
    ↓
Power BI Dashboard
    • Interactive seismic event map
    • Time-series trends (frequency, energy release)
    • Magnitude/depth distribution analysis
    • Top active regions
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Compute** | Databricks (PySpark 3.5) | Distributed data processing |
| **Storage** | Delta Lake | ACID transactions, time travel, efficient MERGE |
| **Orchestration** | Databricks Jobs | File-arrival triggers, scheduled incremental loads |
| **Schema** | Explicit PySpark StructTypes | Type safety, no schema inference |
| **Optimization** | Liquid Clustering, OPTIMIZE | Query performance for BI workloads |
| **BI Layer** | Power BI Desktop + SQL Warehouse | Interactive dashboards, analytical queries |
| **Audit** | Execution/File/Error logs | Observability, debugging, reprocessing |

**Data Source:** [USGS FDSN Event Web Service](https://earthquake.usgs.gov/fdsnws/event/1/) — real-time global earthquake data (magnitude 2.5+)

---

## Medallion Layers

### Bronze Layer: `bronze_seismic_events`

**Purpose:** Immutable raw data landing zone with complete historical versioning

**Schema Highlights:**
- **Primary Key:** `(event_id, valid_from)` — composite key for SCD Type 2
- **Change Detection:** `record_hash = MD5(raw_json)` — only hash changes create new versions
- **Temporal Tracking:** `is_current`, `valid_from`, `valid_to` — point-in-time query capability
- **Partitioning:** `(ingestion_type, year, month)`

**Key Features:**
- Preserves original GeoJSON in `raw_json` column
- Maintains complete audit trail of magnitude updates, status changes
- Re-running identical data inserts 0 rows (hash-based idempotency)
- Enables reconstruction of data state at any past timestamp

**Example:** Event `us6000abc1` magnitude revised from 4.5 to 4.7 creates two versions:
- Version 1: `mag=4.5`, `is_current=FALSE`, `valid_to='2026-10-03'`
- Version 2: `mag=4.7`, `is_current=TRUE`, `valid_to=NULL`

### Silver Layer: `silver_seismic_events`

**Purpose:** Cleansed, typed, business-ready dataset (SCD Type 1)

**Schema Highlights:**
- **Primary Key:** `event_id`
- **Type Enforcement:** All epoch milliseconds → `TIMESTAMP`, strict numeric types
- **Data Quality:** Invalid records → `silver_seismic_events_quarantine`
- **Enrichment:** Derived columns (`country`, `region`, `depth_category`, `magnitude_category`)
- **Partitioning:** `(year, month)` based on `event_time`

**Key Features:**
- MERGE updates only if `event_updated_time` > existing value (timestamp guard)
- Schema drift detection logs unexpected JSON keys to `error_log`
- Failed records quarantined with detailed error context for reprocessing
- `modification_sequence` tracks update count per event

**Data Quality Validations:**
- Latitude: -90 to 90
- Longitude: -180 to 180
- Magnitude: 0 to 10
- Depth: ≥ 0 km

### Gold Layer: Star Schema

**Purpose:** Dimensional model optimized for analytical queries and Power BI

#### Fact Tables
1. **`gold_fact_seismic_event`** — transaction grain (1 row per event)
   - ~166K rows
   - Degenerate dimension: `event_id`
   - Metrics: `magnitude`, `depth_km`, `energy_joules`, `felt_reports`, `significance`
   - Foreign keys: `date_key`, `location_key`, `magnitude_class_key`, `depth_class_key`

2. **`gold_fact_daily_location_summary`** — aggregate grain (1 row per date × location)
   - Pre-aggregated for performance
   - Metrics: `event_count`, `avg_magnitude`, `max_magnitude`, `total_energy_joules`, `shallow/intermediate/deep_count`

#### Dimension Tables
- **`gold_dim_date`** — calendar dimension (~3K rows)
  - Attributes: `year`, `quarter`, `month`, `day_name`, `is_weekend`
- **`gold_dim_location`** — geographic conformance (~4.6K rows)
  - Grain: `(country, state_province)`
  - US state code/name normalization
- **`gold_dim_magnitude_class`** — Richter scale categories (7 rows)
  - Minor, Light, Moderate, Strong, Major, Great
- **`gold_dim_depth_class`** — focal depth categories (3 rows)
  - Shallow (<70km), Intermediate (70-300km), Deep (>300km)

**Physical Optimization:**
- Liquid Clustering: `CLUSTER BY (date_key, location_key)` on facts
- File skipping on filter columns without small-file problems
- Unknown member (`key = -1`) in every dimension eliminates NULL foreign keys

**Refresh Strategy:**
- Incremental watermark-based MERGE: processes only Silver rows with `updated_timestamp > max(silver_updated_timestamp)` in Gold
- Re-running with no new Silver data logs all steps as `SKIPPED` with 0 rows written
- `full_refresh = true` rebuilds from all Silver rows (use after Gold logic changes)

---

## Quick Start

### Prerequisites
- Databricks workspace
- DBFS or Unity Catalog volumes access
- Database/schema creation permissions

### Initial Setup

1. **Create tables and schema**
   ```python
   # Run: notebooks/00_Setup.py
   # Creates: seismo database, 6 tables (Bronze, Silver, Quarantine, 3 audit tables)
   ```

2. **Fetch historical data**
   ```python
   # Run: notebooks/00_Fetch_USGS_Data.py
   # Parameters: fetch_type = "FULL_LOAD", start_year = "2019", end_year = "2024"
   # Output: ~200MB GeoJSON file with 100K+ events
   ```

3. **Run end-to-end pipeline**
   ```python
   # Run: notebooks/05_Run_Pipeline.py
   # Parameters:
   #   ingestion_type = "FULL_LOAD"
   #   source_path = "/Volumes/workspace/seismo/raw/usgs_earthquake_full_load_2019_2024.json"
   #   run_optimize = "true"
   # 
   # Executes: Bronze → Silver → Gold → Optimize
   # Duration: ~14 minutes for full historical load
   ```

### Incremental Updates

```python
# Run: notebooks/05_Run_Pipeline.py
# Parameters:
#   ingestion_type = "INCREMENTAL_LOAD"
#   source_path = ""  # blank = auto-detect newest incremental file
#   run_optimize = "true"
#
# Processes only new/changed events
# Duration: ~14 minutes for typical daily update
```

### Automation

The pipeline can run automatically on file arrival:

1. **Scheduled fetch job:** Runs `00_Fetch_USGS_Data.py` daily (fetches last 24 hours)
2. **File-arrival trigger:** Detects new `.json` files in raw volume
3. **Pipeline job:** Runs `05_Run_Pipeline.py` end-to-end (Bronze → Silver → Gold → Optimize)

Configuration: `jobs/seismo_pipeline_job.yml` + `databricks.yml`

Deployment:
```bash
databricks bundle validate
databricks bundle deploy -t dev
```

---

## Power BI Integration

### Connection Setup
1. Databricks → **SQL Warehouses** → select warehouse → **Connection details**
2. Power BI Desktop → **Get Data** → **Databricks** → paste Server hostname & HTTP path
3. Authentication: OAuth (preferred) or Personal Access Token
4. Import Gold tables: 2 facts + 4 dimensions

### Data Model Configuration
- Mark `gold_dim_date` as the date table (Table tools → Mark as date table)
- Create relationships (all Many-to-One, Single cross-filter):
  - `gold_fact_seismic_event[date_key]` → `gold_dim_date[date_key]`
  - `gold_fact_seismic_event[location_key]` → `gold_dim_location[location_key]`
  - `gold_fact_seismic_event[magnitude_class_key]` → `gold_dim_magnitude_class[magnitude_class_key]`
  - `gold_fact_seismic_event[depth_class_key]` → `gold_dim_depth_class[depth_class_key]`
  - (Similar for `gold_fact_daily_location_summary`)

### Example Visuals
- **Map:** Bubble map with `latitude/longitude`, size = `magnitude`, color = `depth_class`
- **Time series:** Line chart with `full_date`, sum of `event_count`
- **Distribution:** Histogram of `magnitude` frequency
- **Top regions:** Bar chart of `location_label` by `event_count`
- **KPIs:** Cards showing total events, max magnitude, avg magnitude

Detailed setup guide: [`docs/Phase3_Setup_Guide.md`](docs/Phase3_Setup_Guide.md)

---

## Performance & Optimization

### Query Performance
- **Liquid Clustering:** Files organized by `(date_key, location_key)` enable file skipping on common BI filters
- **Broadcast Joins:** All 4 dimensions (<10K rows) broadcast to executors → eliminates shuffle operations
- **Pre-aggregation:** Daily summary fact reduces scans by ~100× for trend analysis
- **Incremental refresh:** Only processes changed Silver rows → typical Gold refresh under 2 minutes

### FinOps Analysis
**Current State (typical incremental run):**
- Duration: 14.2 minutes (850 seconds)
- Cost: $0.036 per run (~4 cents)
- Annual projection (hourly loads): ~$361/year

**Optimization Opportunities:**
1. Parallelize JSON parsing (53% cost reduction) → ~$146/year
2. Reduce OPTIMIZE frequency to daily (8% reduction)
3. Use spot instances (70% reduction) → ~$93/year
4. Batch 6-hour intervals (15% reduction)

**Optimized Target:** ~$108/year (70% savings)

Detailed analysis: [`docs/Phase3_FinOps_SIMPLE.md`](docs/Phase3_FinOps_SIMPLE.md)

---

## Repository Structure

```
seismo/
├── notebooks/
│   ├── config/
│   │   ├── constants.py          # Table names, paths, constants
│   │   ├── schemas.py            # Explicit PySpark StructTypes
│   │   └── utils.py              # Shared utility functions
│   ├── 00_Setup.py               # Initial table creation (run once)
│   ├── 00_Fetch_USGS_Data.py     # API data ingestion
│   ├── 01_Bronze_Ingestion.py    # RAW → BRONZE (SCD2)
│   ├── 02_Silver_transformations.py  # BRONZE → SILVER (SCD1)
│   ├── 03_Gold_Transformations.py    # SILVER → GOLD (star schema)
│   ├── 04_Gold_Optimize.py       # OPTIMIZE + statistics
│   ├── 05_Run_Pipeline.py        # End-to-end orchestrator
│   └── 06_Execution_Analysis.py  # Pipeline observability
├── jobs/
│   └── seismo_pipeline_job.yml   # Databricks Job definitions
├── docs/
│   ├── Gold_Data_Dictionary.md   # Star schema documentation
│   ├── Phase3_Setup_Guide.md     # Deployment & Power BI setup
│   └── Phase3_FinOps_SIMPLE.md   # Cost analysis
├── data/samples/                 # Sample GeoJSON files
├── databricks.yml                # Asset Bundle configuration
└── README.md                     # This file
```

---

## Observability & Debugging

### Execution Monitoring
```sql
-- Latest pipeline run details
SELECT execution_id, layer, notebook_name, status,
       records_processed, records_inserted, records_updated,
       execution_duration_sec
FROM pipeline_execution_log
WHERE batch_id = (
    SELECT batch_id FROM pipeline_execution_log 
    WHERE layer = 'END_TO_END' 
    ORDER BY execution_id DESC LIMIT 1
)
ORDER BY execution_id;
```

### Data Quality Checks
```sql
-- Check quarantined records
SELECT quarantine_reason, COUNT(*) as count
FROM silver_seismic_events_quarantine
GROUP BY quarantine_reason
ORDER BY count DESC;

-- Verify no duplicate events in Silver
SELECT event_id, COUNT(*) as duplicate_count
FROM silver_seismic_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

### Historical Analysis (Bronze SCD2)
```sql
-- Point-in-time query: state of data on Oct 1, 2026
SELECT event_id, 
       GET_JSON_OBJECT(raw_json, '$.properties.mag') AS magnitude,
       valid_from, valid_to
FROM bronze_seismic_events
WHERE valid_from <= TIMESTAMP '2026-10-01 23:59:59'
  AND (valid_to > TIMESTAMP '2026-10-01 23:59:59' OR valid_to IS NULL)
LIMIT 100;

-- Track magnitude updates for a specific event
SELECT event_id, 
       GET_JSON_OBJECT(raw_json, '$.properties.mag') AS magnitude,
       GET_JSON_OBJECT(raw_json, '$.properties.status') AS status,
       is_current, valid_from, valid_to
FROM bronze_seismic_events
WHERE event_id = 'us6000abc1'
ORDER BY valid_from;
```

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Schema enforcement** | Explicit PySpark StructTypes, zero `inferSchema` | Type safety, predictable behavior, production requirement |
| **Bronze versioning** | SCD Type 2 | USGS updates magnitude/status days after event; need audit trail |
| **Silver grain** | Latest version only (SCD Type 1) | Analytical queries want current truth; history available in Bronze |
| **Gold shape** | Star (not snowflake) | Simple one-hop joins, fast BI queries, DAX-friendly |
| **Surrogate keys** | Integer keys (`date_key=yyyyMMdd`, sequential for location) | Fast joins, small storage footprint, human-readable dates |
| **Unknown members** | Key `-1` in every dimension | Prevent NULL foreign keys, never drop fact rows in Power BI |
| **Location conformance** | `(country, state_province)` grain | Silver `region` too granular (almost unique); standardize US states |
| **Physical layout** | Liquid Clustering (no Hive partitions) | ~15K events/year → partitions create small files; clustering gives file skipping without fragmentation |
| **Incremental refresh** | Watermark on `updated_timestamp` | Process only changed Silver rows; re-run with no changes writes 0 rows |
| **Energy calculation** | Gutenberg-Richter: `10^(1.5*M + 4.8)` | Enables "energy vs frequency" analysis (logarithmic scale → joules) |

---

## Idempotency Guarantees

Every layer is fully idempotent — re-running the same batch with identical data writes **0 rows**:

- **Bronze:** Hash-based change detection (`MD5(raw_json)`) — unchanged events skipped in MERGE
- **Silver:** Timestamp guard (`event_updated_time` must be newer) — same timestamp = no update
- **Gold:** Watermark-based read (`updated_timestamp > last_processed`) — no new Silver data = SKIPPED

Execution guard: Successfully completed batches require `force=true` to re-run (failed batches can always re-run).

---

## Data Dictionary

Complete schema documentation with column definitions, data types, foreign key relationships, and ER diagrams:

- **Gold Layer:** [`docs/Gold_Data_Dictionary.md`](docs/Gold_Data_Dictionary.md)

---

## Academic Context

This project was developed in three phases for a university Data Engineering course:

- **Phase 1:** Proposal — data source selection, architecture design, analytical questions
- **Phase 2:** Bronze + Silver — Medallion architecture, SCD Type 2, idempotency, audit framework
- **Phase 3:** Gold + BI — star schema design, Power BI dashboards, Spark optimization analysis, FinOps evaluation

The README presents the complete pipeline as a cohesive production system rather than three disconnected assignments.

---

## License

Academic project — FAST-NUCES, DS-3001 Data Analysis and Visualization

**Data Source:** USGS earthquake data is public domain
