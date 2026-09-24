# 🌍 Automated USGS Seismic Event Telemetry Pipeline

**DS-3001 Data Engineering Project | FAST-NUCES**

> Global earthquake monitoring and real-time seismic event tracking using Databricks Medallion Architecture

---

## 📋 Project Overview

This project builds an **automated data engineering pipeline** for global seismic event monitoring, tracking earthquake occurrences, magnitude distributions, focal depths, and geographic fault-line activity. The pipeline ingests data from the USGS (United States Geological Survey) and processes it through a **Bronze → Silver → Gold** Medallion architecture to provide refined operational data for disaster risk analysis and geological research.

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
│       ├── usgs_earthquake_full_load.json         # Historical baseline (14,048 events)
│       └── usgs_earthquake_incremental.json       # Daily incremental updates
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

The ingestion script performs two operations:

1. **Full Load:** Fetches historical earthquake data (2-year baseline, magnitude ≥ 4.5)
   - Time period: 2023-01-01 to 2025-01-01
   - Output: `data/samples/usgs_earthquake_full_load.json`
   - ~14,000+ events, ~15 MB

2. **Incremental Load:** Fetches events updated in the last 24 hours
   - Uses `updatedafter` parameter
   - Output: `data/samples/usgs_earthquake_incremental.json`
   - ~600+ events per day, ~0.7 MB

### Sample Data Verification

After running the script, you should see output like:

```
============================================================
USGS EARTHQUAKE DATA INGESTION
DS-3001 Data Engineering Project - Phase 1
============================================================

============================================================
FULL LOAD: Fetching historical data
Period: 2023-01-01 to 2025-01-01
Minimum Magnitude: 4.5
============================================================

✓ Success!
  • Events fetched: 14,048
  • File size: 15.12 MB
  • Saved to: data/samples/usgs_earthquake_full_load.json

============================================================
SAMPLE EVENT DETAILS (First Event)
============================================================

Event ID:     us6000pgri
Magnitude:    5.0 mwr
Location:     49 km W of Puerto, Chile
Depth:        35.00 km
Coordinates:  [-20.1826, -70.6231]
Time:         2025-01-01 04:13:20 UTC
Status:       reviewed
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

### Full Load
- **Volume:** ~150 MB – 250 MB (2-year period, magnitude ≥ 4.5)
- **Frequency:** One-time historical baseline
- **Event Count:** ~14,000 seismic events (2023-2025)

### Incremental Load
- **Volume:** ~2 MB – 5 MB per day
- **Frequency:** Daily automated polling
- **Event Count:** ~500-1,000 events/day

### Spark & FinOps Fit
- Highly manageable for PySpark in-memory execution
- Well within Databricks Free Edition limits (15 GB driver memory)
- Sufficient scale for multi-year trend analysis

---

## 🎨 Business Intelligence & Dashboards (Phase 3)

### Key Questions Answered

1. Which geographical fault lines exhibit accelerating seismic activity over time?
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
- **Workflow:** Fetch data → Upload to DBFS Volumes → Process in Databricks

### Cluster Optimization
- Auto-termination set to 20 minutes of inactivity
- Delta tables partitioned by year and month
- Optimized scan queries to conserve compute credits

---

## 🗓️ Project Timeline

| Phase | Deliverable | Due Date |
|-------|-------------|----------|
| **Phase 1** | Proposal + GitHub Repo + Sample Data | Sep 26, 2026 |
| **Phase 2** | Bronze/Silver/Gold Notebooks | Oct 10, 2026 |
| **Phase 3** | BI Dashboard + Final Report | Oct 24, 2026 |

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

**Last Updated:** September 24, 2026
