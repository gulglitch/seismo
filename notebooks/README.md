# Databricks Notebooks

This directory will contain PySpark notebooks for data processing in Databricks.

## Planned Notebooks (Phase 2 & 3):

### 1. Bronze Layer Processing
- **Notebook:** `01_bronze_ingestion.py`
- **Purpose:** Load raw GeoJSON files from DBFS into Bronze Delta Lake tables
- **Operations:**
  - Read JSON files from DBFS Volumes
  - Add ingestion metadata (batch_id, ingested_at, source_file)
  - Store as Delta Lake with append-only writes

### 2. Silver Layer Transformation
- **Notebook:** `02_silver_transformation.py`
- **Purpose:** Cleanse and standardize data
- **Operations:**
  - Flatten nested GeoJSON structure
  - Convert epoch timestamps to UTC TimestampType
  - Cast coordinates and magnitude to DoubleType
  - Apply MERGE logic for updates/deletions based on event_id
  - Parse location strings into country/province codes

### 3. Gold Layer Analytics
- **Notebook:** `03_gold_analytics.py`
- **Purpose:** Create dimensional models and aggregations
- **Operations:**
  - Build star schema (FactSeismicEvent, DimLocation, DimMagnitudeClass)
  - Calculate KPIs (monthly event frequency, depth vs magnitude distributions)
  - Create seismic risk indexes

## Setup Instructions (Phase 2)

1. Upload notebooks to Databricks workspace
2. Connect to Databricks cluster
3. Mount DBFS volumes containing raw data
4. Execute notebooks in sequence: Bronze → Silver → Gold

---

**Note:** These notebooks will be developed in Phase 2 after Phase 1 proposal approval.
