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

## 🏗️ Architecture

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
