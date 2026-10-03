# DS-3001 Data Engineering — Project Phase 1 Proposal

## Seismo: Global Seismic Event Tracking & Real-Time USGS Earthquake Pipeline

---

**Project Title:** Seismo - Automated USGS Seismic Event Telemetry Pipeline

**Course:** DS-3001 Data Analysis and Visualization  
**Institution:** FAST-NUCES  
**Due Date:** September 26, 2026  

**Team Members:**
- Gul-e-Zara (Roll No: 24L-2592)
- Maria Shahab (Roll No: 23L-2592)

**GitHub Repository:** https://github.com/gulglitch/seismo

---

## Project Overview

**Target Architecture:** Databricks Medallion (Bronze → Silver → Gold) with Decoupled Ingestion

This project builds an automated data engineering pipeline for global seismic event monitoring, tracking earthquake occurrences, magnitude distributions, focal depths, and geographic patterns. The goal is to provide refined operational data for disaster risk analysis and geological research.

---

## 1. Domain & Source Identification

### Project Concept

**Seismo** automates the collection and processing of global earthquake data to support:
- Real-time disaster risk assessment
- Geological research on seismic patterns
- Regional seismic activity monitoring
- Earthquake magnitude and depth analysis

### Data Source

**United States Geological Survey (USGS) FDSN Event Web Service REST API**

- **API Endpoint:** `https://earthquake.usgs.gov/fdsnws/event/1/query`
- **Format:** GeoJSON (structured JSON with geographic features)
- **Authentication:** None required (public API)
- **Reliability:** Highly reliable, maintained by U.S. government
- **Update Frequency:** Real-time (events updated as seismologists review data)

### Ingestion Pattern & Load Types

#### Full Load
The ability to extract a large historical baseline dataset for initial pipeline setup and analysis.

- **Time Period:** 2023-01-01 to 2025-01-01 (2-year baseline)
- **API Parameters:** `starttime`, `endtime`, `minmagnitude=4.5`
- **Purpose:** Establish historical baseline for trend analysis
- **Frequency:** One-time or periodic (monthly/quarterly for rebuilds)

#### Incremental Load
The ability to fetch new, updated, or deleted records periodically to keep the pipeline current.

- **API Parameter:** `updatedafter` (ISO timestamp)
- **Captures:**
  - Newly registered seismic events
  - Re-evaluated event magnitudes (manual review updates)
  - Deleted event IDs (reclassified records with `status='deleted'`)
- **Purpose:** Daily updates to maintain fresh data
- **Frequency:** Daily automated polling

---

## 2. Data Samples & Volume

### Sample Files

Sample raw data files have been acquired and pushed to the GitHub repository under `/data/samples/`:

#### Full Load Sample
- **File:** `usgs_earthquake_full_load.json`
- **Description:** Baseline historical GeoJSON feature collection
- **Time Range:** January 1, 2023 to January 1, 2025
- **Event Count:** 14,048 seismic events
- **File Size:** 15.12 MB

#### Incremental Load Sample
- **File:** `usgs_earthquake_incremental.json`
- **Description:** Daily incremental GeoJSON batch containing updates and new events
- **Time Range:** Last 24 hours (from September 23-24, 2026)
- **Event Count:** 666 events
- **File Size:** 0.72 MB

### Volume & Frequency Estimation

#### Full Load Volume
- **Initial Load:** ~15 MB (GeoJSON format, magnitude ≥ 4.5 filter)
- **Event Count:** ~14,000 events over 2-year period
- **Rationale:** Magnitude filter (≥ 4.5) focuses on earthquakes with disaster significance, excluding micro-seismic noise

#### Incremental Load Volume
- **Daily Volume:** ~0.7 - 1.5 MB per day
- **Event Count:** ~500-800 events per daily pull
- **Annual Growth:** ~250-500 MB per year

#### Spark & FinOps Fit
- **In-Memory Processing:** Highly manageable for PySpark execution
- **Databricks Compatibility:** Well within Databricks Community Edition limits (15 GB driver memory)
- **Scalability:** Sufficient volume for multi-year trend analysis while remaining cost-effective

---

## 3. Security & Compliance

### PII & Sensitive Data Identification

**Audit Result:** The USGS Earthquake dataset contains **zero Personally Identifiable Information (PII)**.

The data consists strictly of:
- Physical geography (latitude, longitude, depth)
- Sensor readings (magnitude, intensity, seismic wave measurements)
- Station metadata (number of reporting stations, network codes)
- UTC timestamps
- Event status codes

**No human-identifiable information exists in this dataset.**

### Handling Strategy

Since no PII exists, masking or hashing is not required. However, we will implement the following data sanitization:

- **Location Strings:** Raw descriptive strings (e.g., "12km ENE of Muzaffarabad, Pakistan") will be parsed in the Silver layer into standardized country and province boundary codes
- **Data Validation:** Remove or flag anomalous readings (depth = -999, magnitude = null)
- **Status Filtering:** Implement soft-delete logic for events marked with `status='deleted'`

---

## 4. High-Level Medallion Data Modeling

The pipeline implements a **3-tier Medallion Architecture** on Delta Lake:

### Bronze Layer (Raw)

**Format:** Delta Lake / DBFS (Append-Only)

**Processing Logic:**
- Store raw GeoJSON feature objects directly from API
- Append ingestion metadata:
  - `ingested_at`: Timestamp of data ingestion
  - `source_file`: Origin file name
  - `batch_id`: Unique identifier for each ingestion run
- No transformations — preserve data lineage
- Enable time travel for audit trails

### Silver Layer (Cleansed)

**Format:** Delta Lake (Standardized Schema)

**Transformation Logic:**

1. **Flatten Nested JSON:**
   - Extract properties from GeoJSON `properties` object into explicit columns
   - Separate `geometry.coordinates` into `latitude`, `longitude`, `depth`

2. **Type Conversions:**
   - Convert epoch millisecond timestamps → `TimestampType` (UTC)
   - Cast coordinates (lat, lon, depth) → `DoubleType`
   - Cast magnitude → `DoubleType`

3. **Data Standardization:**
   - Parse location strings into structured fields (country, province, distance, direction)
   - Normalize magnitude types (mb, ml, mw, etc.) with standardization logic
   - Categorize depth ranges: Shallow (<70 km), Intermediate (70-300 km), Deep (>300 km)

4. **Upsert Logic:**
   - Apply Spark `MERGE` operation on `event_id` to handle:
     - New events (INSERT)
     - Updated magnitudes or status (UPDATE)
     - Soft-delete for `status='deleted'` records (mark `is_active=false`)

### Gold Layer (Analytics)

**Format:** Delta Lake (Star Schema)

**Data Model:**

#### Fact Table: `FactSeismicEvent`
- `event_id` (PK)
- `location_key` (FK to DimLocation)
- `magnitude_class_key` (FK to DimMagnitudeClass)
- `event_timestamp`
- `magnitude`
- `depth`
- `latitude`
- `longitude`
- `felt_reports`
- `tsunami_flag`
- `seismic_stations_count`

#### Dimension Table: `DimLocation`
- `location_key` (PK)
- `country`
- `province`
- `region`

#### Dimension Table: `DimMagnitudeClass`
- `magnitude_class_key` (PK)
- `magnitude_range` (e.g., "4.5-4.9")
- `richter_category` (Moderate, Strong, Major, Great)
- `typical_damage_level`

#### Dimension Table: `DimTime`
- `time_key` (PK)
- `date`
- `year`
- `month`
- `quarter`
- `day_of_week`

**Pre-Aggregated KPIs:**
- Monthly event frequency by region
- Average magnitude by depth category
- Seismic energy release calculations
- Regional seismic activity indexes

---

## 5. Business Intelligence & Dashboards

### Visual Insights

The final Power BI dashboard will answer critical geological and disaster risk questions:

#### Key Questions
1. Which geographical regions exhibit accelerating seismic activity over time?
2. What is the ratio of shallow-focus (<70 km) to deep-focus (>300 km) earthquakes across regions?
3. How does seismic energy release correlate with earthquake frequency?
4. Which regions show increasing magnitude trends that may indicate higher disaster risk?

### Delivered Dashboard Visuals (Power BI)

#### 1. **Seismic Event Map (Interactive Bubble Map)**
- **Visual Type:** Power BI Map or ArcGIS Map
- **Data Mapping:**
  - **Location:** Latitude/Longitude from `FactSeismicEvent`
  - **Bubble Size:** Magnitude (larger bubbles = higher magnitude)
  - **Color:** Depth category (Shallow = Red, Intermediate = Yellow, Deep = Blue)
  - **Tooltip:** Event details (place, magnitude, depth, timestamp)
- **Interactivity:** 
  - Click region to filter other visuals
  - Drill-through to event details
  - Time slider for animation over months/years

#### 2. **Time-Series: Monthly Event Frequency & Energy Release**
- **Visual Type:** Line and Column Chart (Combo)
- **X-Axis:** Month/Year (from `DimTime`)
- **Y-Axis (Left):** Event count (column bars)
- **Y-Axis (Right):** Cumulative seismic energy released (line)
- **Formula:** Energy = 10^(1.5 × Magnitude + 4.8) joules
- **Filters:** Country, magnitude class, depth category
- **Insight:** Shows whether increasing frequency correlates with energy release

#### 3. **Magnitude Distribution Histogram**
- **Visual Type:** Clustered Column Chart
- **X-Axis:** Magnitude classes (4.5-4.9, 5.0-5.9, 6.0-6.9, 7.0+)
- **Y-Axis:** Count of events
- **Color Breakdown:** By Richter category (Moderate, Strong, Major, Great)
- **Filters:** Date range, region
- **Insight:** Understand earthquake severity distribution

#### 4. **Depth vs Magnitude Scatter Plot**
- **Visual Type:** Scatter Chart
- **X-Axis:** Depth (km)
- **Y-Axis:** Magnitude
- **Bubble Size:** Felt reports count
- **Color:** Region or country
- **Trend Line:** Regression line showing correlation
- **Insight:** Identify patterns between depth and impact severity

#### 5. **Top 10 Most Active Regions (Bar Chart)**
- **Visual Type:** Horizontal Bar Chart
- **Y-Axis:** Region / Country names
- **X-Axis:** Total event count (past 2 years)
- **Color:** Average magnitude (gradient: low = green, high = red)
- **Tooltip:** Latest event date, max magnitude recorded
- **Insight:** Prioritize high-risk zones for monitoring

#### 6. **KPI Cards (Summary Metrics)**
- **Visual Type:** Card / Multi-row Card
- **Metrics:**
  - Total Events (2023-2025)
  - Average Magnitude
  - Highest Magnitude Recorded
  - Events in Last 30 Days
  - Regions Affected
- **Conditional Formatting:** Red alert if recent activity exceeds historical average

---

## 6. Engineering Setup & FinOps Awareness

### Version Control & Repository Structure

A public GitHub repository is maintained with the following structure:

```
seismo/
├── data/
│   └── samples/
│       ├── usgs_earthquake_full_load.json      # 14,048 events
│       └── usgs_earthquake_incremental.json    # 666 events
├── src/
│   └── ingestion/
│       ├── fetch_usgs_data.py                  # Python REST API script
│       └── requirements.txt                    # Dependencies
├── notebooks/
│   └── README.md                               # Databricks notebook placeholders
├── README.md                                   # Architecture documentation
└── .gitignore
```

**Repository Link:** https://github.com/gulglitch/seismo

### FinOps & Infrastructure Management

#### Outbound Network Workaround
**Challenge:** Databricks Community Edition blocks external API calls.

**Solution:** Decouple ingestion from Databricks:
1. Run Python ingestion script locally or via GitHub Actions
2. Fetch GeoJSON payloads from USGS API
3. Upload files to Databricks DBFS Volumes
4. Databricks notebooks read from DBFS (not API directly)

#### Cluster & Storage Optimization
- **Auto-Termination:** Databricks compute clusters set to terminate after 20 minutes of inactivity
- **Delta Lake Partitioning:** Tables partitioned by `year` and `month` to optimize scan queries
- **Z-Ordering:** Apply Z-ORDER on `magnitude` and `country` for faster BI queries
- **Vacuum Operations:** Regular cleanup of old Delta Lake versions to conserve storage

#### Cost Management
- **Databricks Community Edition:** Free tier with 15 GB driver memory
- **Spark Processing:** Dataset size (15 MB full load + daily 1 MB incremental) is well within free-tier limits
- **Storage:** DBFS provides sufficient capacity for academic use
- **Monitoring:** Track compute usage to stay within free tier boundaries

---

## 7. Technology Stack

| Component | Technology |
|-----------|-----------|
| **Data Ingestion** | Python 3.8+, Requests library |
| **Data Storage** | Delta Lake (Databricks) |
| **Data Processing** | Apache Spark (PySpark) |
| **Orchestration** | Databricks Notebooks (manual/scheduled runs) |
| **Version Control** | Git / GitHub |
| **Business Intelligence** | Power BI Desktop |
| **Infrastructure** | Databricks Community Edition |

---

## 8. Project Timeline

| Phase | Deliverables | Due Date |
|-------|--------------|----------|
| **Phase 1** | Proposal document + GitHub repo + Sample data files | Sep 26, 2026 |
| **Phase 2** | Bronze/Silver/Gold PySpark notebooks | Oct 10, 2026 |
| **Phase 3** | Power BI dashboard + Final report | Oct 24, 2026 |

---

## 9. Success Criteria

### Phase 1 (Current)
- ✅ GitHub repository with sample data
- ✅ Full load dataset (14,048 events)
- ✅ Incremental load dataset (666 events)
- ✅ Python ingestion script (working)
- ✅ Comprehensive documentation

### Phase 2
- Databricks notebooks for Bronze/Silver/Gold layers
- Delta Lake tables with proper schema
- MERGE logic for incremental updates
- Data quality validation

### Phase 3
- Power BI dashboard with 6 interactive visuals
- Fact and dimension tables in Gold layer
- Performance optimization (query times < 5 seconds)
- Final presentation and report

---

## 10. Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| API rate limits or downtime | Cannot fetch data | Cache sample data locally; use USGS bulk download as backup |
| Databricks free tier limitations | Cannot process large datasets | Apply magnitude filter (≥4.5) to reduce volume; partition data |
| Data quality issues (null values, outliers) | Inaccurate analysis | Implement validation in Silver layer; flag anomalies |
| Complex Power BI visualizations | Learning curve | Use built-in visuals; reference Power BI documentation |

---

## Conclusion

**Seismo** demonstrates modern data engineering practices using the Medallion Architecture pattern. By automating the ingestion, transformation, and analysis of USGS earthquake data, we provide actionable insights for disaster risk management and geological research. The decoupled ingestion strategy ensures compatibility with Databricks Community Edition, while the star schema in the Gold layer enables efficient BI reporting in Power BI.

---

**Submitted by:**
- Gul-e-Zara (24L-2592)
- Maria Shahab (23L-2592)

**Date:** September 24, 2026

**GitHub Repository:** https://github.com/gulglitch/seismo
