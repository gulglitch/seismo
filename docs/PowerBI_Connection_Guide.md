# Power BI to Databricks Connection Guide — Seismo Project

This guide walks you through connecting Power BI Desktop to your Databricks workspace and building the Phase 3 dashboard.

---

## Prerequisites

### 1. Software Required
- **Power BI Desktop** (free download from Microsoft Store or powerbi.microsoft.com)
- Active Databricks workspace with running cluster

### 2. Databricks Information You'll Need

| Item | Where to Find It |
|------|------------------|
| **Server Hostname** | Compute → Your Cluster → Advanced Options → JDBC/ODBC → Server Hostname |
| **HTTP Path** | Same location → HTTP Path |
| **Access Token** | User Settings (top right) → Developer → Access Tokens → Generate New Token |
| **Schema Name** | `seismo` (your database name) |

---

## Step 1: Get Connection Details from Databricks

### A. Start Your Cluster
1. Go to **Compute** in Databricks sidebar
2. Start your cluster if stopped
3. Wait for "Running" status

### B. Get Server Hostname and HTTP Path
```
Navigation: Databricks → Compute → [Click Your Cluster] → Configuration tab
→ Advanced Options → JDBC/ODBC tab

Copy both values:
- Server Hostname: looks like adb-1234567890.azuredatabricks.net
- HTTP Path: looks like /sql/1.0/warehouses/abc123
```

### C. Generate Access Token
```
Click your profile icon (top right) → User Settings → Developer
→ Access Tokens → Generate New Token

Settings:
- Comment: "Power BI Dashboard"
- Lifetime: 90 days
- Click Generate
- COPY TOKEN IMMEDIATELY (can't view again!)
```

---

## Step 2: Connect Power BI to Databricks

### A. Open Power BI Desktop

### B. Get Data
1. Click **Home** → **Get Data** → **More...**
2. Search: **"Azure Databricks"**
3. Select **Azure Databricks** connector
4. Click **Connect**

### C. Enter Connection Details
```
Server Hostname: [paste from Step 1B]
HTTP Path: [paste from Step 1B]

Data Connectivity mode:
- DirectQuery (recommended - live connection, always fresh data)
- OR Import (faster visuals, but need manual refresh)
```
Click **OK**

### D. Authenticate
1. Choose **Personal Access Token**
2. Paste your token from Step 1C
3. Click **Connect**

### E. Select Tables
In Navigator window:
- Expand `hive_metastore` (or your catalog)
- Expand `seismo`
- **Check these tables:**
  - ✅ gold_fact_seismic_event
  - ✅ gold_fact_daily_location_summary
  - ✅ gold_dim_date
  - ✅ gold_dim_location
  - ✅ gold_dim_magnitude_class
  - ✅ gold_dim_depth_class
  - ✅ gold_vw_pipeline_runs
  - ✅ gold_vw_error_summary

Click **Load**

---

## Step 3: Configure the Star Schema (CRITICAL!)

### A. Open Model View
- Click **Model** icon (left sidebar, looks like linked tables)

### B. Create Relationships

Drag from fact tables to dimension tables:

**From gold_fact_seismic_event:**
1. `date_key` → `gold_dim_date[date_key]` (Many to One)
2. `location_key` → `gold_dim_location[location_key]` (Many to One)
3. `magnitude_class_key` → `gold_dim_magnitude_class[magnitude_class_key]` (Many to One)
4. `depth_class_key` → `gold_dim_depth_class[depth_class_key]` (Many to One)

**From gold_fact_daily_location_summary:**
5. `date_key` → `gold_dim_date[date_key]` (Many to One)
6. `location_key` → `gold_dim_location[location_key]` (Many to One)

**Verify each relationship:**
- Cardinality: **Many to one (*:1)**
- Cross filter: **Single**

### C. Mark Date Table
1. Right-click `gold_dim_date`
2. **Mark as date table**
3. Choose `full_date` column
4. Click OK

### D. Set Sort Columns
**In gold_dim_date:**
- `month_name` → Sort by `month`
- `day_name` → Sort by `day_of_week`  
- `year_month` → Sort by `year_month_key`

**In gold_dim_magnitude_class:**
- `magnitude_class` → Sort by `sort_order`

**In gold_dim_depth_class:**
- `depth_class` → Sort by `sort_order`

---

## Step 4: Create Dashboard Visuals (Minimum 3 Required)

### Page 1: Seismic Activity Overview

#### KPI Cards (Top Row)
Create 4 **Card** visuals:

1. **Total Events**
   - Field: `gold_fact_seismic_event[event_count]` (Sum)

2. **Average Magnitude**
   - Field: `gold_fact_seismic_event[magnitude]` (Average)

3. **Events Last 30 Days**
   - Field: `gold_fact_seismic_event[event_count]` (Sum)
   - Filter: `gold_dim_date[full_date]` → Last 30 days

4. **Active Locations**
   - Create measure:
     ```DAX
     Active Locations = DISTINCTCOUNT(gold_fact_seismic_event[location_key])
     ```

#### Visual 1: Events Over Time (Required)
- **Type:** Line chart
- **X-axis:** `gold_dim_date[year_month]`
- **Y-axis:** `gold_fact_daily_location_summary[event_count]` (Sum)
- **Title:** "Monthly Seismic Activity Trend"

#### Visual 2: Top Locations (Required)
- **Type:** Bar chart (horizontal)
- **Y-axis:** `gold_dim_location[location_label]`
- **X-axis:** `gold_fact_seismic_event[event_count]` (Sum)
- **Filter:** Top 10 locations
- **Title:** "Top 10 Most Active Regions"

#### Visual 3: Magnitude Distribution (Required)
- **Type:** Clustered column chart
- **X-axis:** `gold_dim_magnitude_class[magnitude_class]`
- **Y-axis:** `gold_fact_seismic_event[event_count]` (Sum)
- **Title:** "Events by Magnitude Class"

#### Additional Visuals (Recommended)

**Energy vs Frequency:**
- **Type:** Scatter chart
- **X-axis:** `gold_fact_daily_location_summary[event_count]`
- **Y-axis:** `gold_fact_daily_location_summary[total_energy_joules]`
- **Details:** `gold_dim_location[location_label]`

**Depth Distribution:**
- **Type:** Donut chart
- **Legend:** `gold_dim_depth_class[depth_class]`
- **Values:** `gold_fact_seismic_event[event_count]` (Sum)

**Map Visual:**
- **Type:** Map
- **Location:** Use latitude/longitude from fact table
- **Size:** `gold_fact_seismic_event[magnitude]`

### Page 2: Pipeline Health (Audit Dashboard)

#### Pipeline Runs
- **Type:** Table
- **Columns:** 
  - `gold_vw_pipeline_runs[notebook_name]`
  - `gold_vw_pipeline_runs[status]`
  - `gold_vw_pipeline_runs[execution_duration_sec]`
  - `gold_vw_pipeline_runs[records_processed]`

#### Error Trend
- **Type:** Line chart
- **X-axis:** `gold_vw_error_summary[date_key]`
- **Y-axis:** Count of errors
- **Legend:** `gold_vw_error_summary[layer]`

---

## Step 5: Format and Save

### Formatting Tips
1. **Apply theme:** View → Themes → Choose professional theme
2. **Align visuals:** Format → Align → Distribute evenly
3. **Add title:** Insert → Text box → "Seismo: Global Earthquake Analytics"
4. **Add filters:** Use slicers for date range, location type

### Save Your Work
1. **File → Save As**
2. Name: `Seismo_Phase3_Dashboard.pbix`
3. Location: `e:\Projects\seismo\`

---

## Troubleshooting

### Issue: "Can't connect to data source"
**Solution:**
- Verify cluster is running in Databricks
- Check access token hasn't expired
- Confirm Server Hostname and HTTP Path are correct

### Issue: "No tables visible in Navigator"
**Solution:**
- Make sure you've run the Gold layer notebooks
- Check you're connected to correct schema (`seismo`)
- Verify tables exist: Run `SHOW TABLES IN seismo` in Databricks

### Issue: "Relationships not working"
**Solution:**
- Verify foreign keys match between fact and dimension
- Check cardinality is Many-to-One
- Ensure date table is marked correctly

### Issue: "Slow performance"
**Solution:**
- Use DirectQuery instead of Import for large datasets
- Apply filters to reduce data volume
- Consider aggregating in DAX measures

---

## Export for Submission

### For Phase 3 Submission:
1. **Save .pbix file:** File → Save As → `Seismo_Phase3_Dashboard.pbix`
2. **Take screenshots:** 
   - Full dashboard view
   - Model view showing relationships
   - Spark UI from Databricks
3. **Export to PDF (optional):** File → Export to PDF

---

## Next Steps

After completing the dashboard:
1. ✅ Test all visuals work correctly
2. ✅ Verify relationships in Model view
3. ✅ Take screenshots for your report
4. ✅ Document dashboard design decisions
5. ✅ Write the Final Architecture Report (see Phase3_Requirements.md)

**Questions?** Review the Gold Data Dictionary for business question mappings.
