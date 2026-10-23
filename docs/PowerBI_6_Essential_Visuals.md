# Power BI Dashboard - 6 Essential Visuals Guide

**Your Progress:** ✅ 5 KPI Cards completed!  
**Remaining:** 6 core visuals to complete your Phase 3 dashboard

---

## 📊 **Card vs KPI Visual - What's the Difference?**

| Feature | **Card** (what you used ✅) | **KPI** |
|---------|---------------------------|---------|
| Shows | Single number | Number + target + trend |
| Best for | Simple metrics | Goals with targets |
| Trend indicator | No | Yes (up/down arrows) |
| Your use | ✅ Perfect! | Overkill for this project |

**You made the right choice!** Card visuals are cleaner for showing simple metrics.

---

## 🎯 **Your 6 Essential Visuals**

### **Visual 1: Map - Earthquake Locations** ⭐ (Answers Q1 & Q4)
**Type:** Map (the standard one you see)
- **Visual pane → Map icon** (globe icon)

**Configuration:**
- **Location:** Drag BOTH fields:
  - `gold_fact_seismic_event[latitude]`
  - `gold_fact_seismic_event[longitude]`
- **Size:** `gold_fact_seismic_event[magnitude]`
- **Legend (Color):** `gold_dim_depth_class[depth_class]`
- **Tooltip:** Add these for hover details:
  - `gold_fact_seismic_event[place]`
  - `gold_fact_seismic_event[magnitude]`
  - `gold_fact_seismic_event[depth_km]`
  - `gold_dim_date[full_date]`

**Formatting:**
- Format → Data colors → Choose depth class colors:
  - Shallow = Red
  - Intermediate = Orange  
  - Deep = Blue

**Purpose:** Shows WHERE earthquakes happen with size by magnitude and color by depth

---

### **Visual 2: Line Chart - Monthly Event Trend** ⭐ (Answers Q1 - Acceleration)
**Type:** Line chart

**Configuration:**
- **X-axis:** `gold_dim_date[year_month]`
- **Y-axis:** `gold_fact_daily_location_summary[event_count]` (Sum)
- **Legend (optional):** `gold_dim_location[location_type]`

**Formatting:**
- Data labels → On (shows values on line)
- Title: "Monthly Seismic Activity Trend"

**Purpose:** Shows if earthquake frequency is INCREASING over time

---

### **Visual 3: Scatter Chart - Energy vs Frequency** ⭐ (Answers Q3 - Correlation)
**Type:** Scatter chart

**Configuration:**
- **X-axis:** `gold_fact_daily_location_summary[event_count]` (Sum)
- **Y-axis:** `gold_fact_daily_location_summary[total_energy_joules]` (Sum)
- **Values/Details:** `gold_dim_location[location_label]`
- **Size:** `gold_fact_daily_location_summary[avg_magnitude]`
- **Legend:** `gold_dim_location[location_type]`

**Add Trend Line:**
1. Click on the visual
2. Analytics pane (magnifying glass icon)
3. Trend line → Add
4. Choose Linear

**Formatting:**
- Y-axis → Display units → Scientific notation

**Purpose:** Does more earthquakes = more energy? Shows Gutenberg-Richter relationship

---

### **Visual 4: Stacked Bar Chart - Shallow vs Deep by Region** ⭐ (Answers Q2 - CRITICAL!)
**Type:** Stacked bar chart (horizontal)

**Configuration:**
- **Y-axis:** `gold_dim_location[location_label]`
- **X-axis:** Count (or use measure below)
- **Legend:** `gold_dim_depth_class[depth_class]`

**Filter:** 
- Filters pane → Add filter on `gold_dim_location[location_label]`
- Filter type: Top N
- Show items: Top 10
- By value: `SUM(gold_fact_seismic_event[event_count])`

**Purpose:** Which regions have more shallow (dangerous) earthquakes?

---

### **Visual 5: Clustered Column Chart - Magnitude Distribution** (Answers Q4)
**Type:** Clustered column chart

**Configuration:**
- **X-axis:** `gold_dim_magnitude_class[magnitude_class]`
- **Y-axis:** `gold_fact_seismic_event[event_count]` (Sum)

**Formatting:**
- Data colors → Conditional formatting:
  - Minor = Green
  - Light = Yellow
  - Moderate = Orange
  - Strong = Dark Orange
  - Major/Great = Red
- Sort by: `gold_dim_magnitude_class[sort_order]` (ensures correct order)
- Title: "Events by Magnitude Class"

**Purpose:** Distribution of earthquake severity

---

### **Visual 6: Clustered Bar Chart - Top 10 Active Regions**
**Type:** Clustered bar chart (horizontal)

**Configuration:**
- **Y-axis:** `gold_dim_location[location_label]`
- **X-axis:** `gold_fact_seismic_event[event_count]` (Sum)
- **Tooltip:** Add `AVERAGE(gold_fact_seismic_event[magnitude])`

**Filter:**
- Filters pane → Top N filter
- Show: Top 10 locations
- By: Event count

**Formatting:**
- Data colors → Gradient based on values (darker = more events)
- Data labels → On
- Title: "Top 10 Most Active Seismic Regions"

**Purpose:** Which regions have the most earthquakes?

---

## 📄 **PAGE 2: Pipeline Health** (2 Additional Audit Visuals)

Phase 3 requires visualizations of audit/error logs. Add these on a second page:

### **Audit Visual 1: Table - Pipeline Execution Summary**
**Type:** Table

**Columns:**
- `gold_vw_pipeline_runs[execution_start]`
- `gold_vw_pipeline_runs[layer]`
- `gold_vw_pipeline_runs[status]`
- `gold_vw_pipeline_runs[records_processed]`
- `gold_vw_pipeline_runs[execution_duration_sec]`

**Filter:** Last 30 days

---

### **Audit Visual 2: Stacked Column Chart - Pipeline Status**
**Type:** Stacked column chart

**Configuration:**
- **X-axis:** `gold_vw_pipeline_runs[date_key]` (as date)
- **Y-axis:** Count of runs
- **Legend:** `gold_vw_pipeline_runs[status]`

**Data colors:**
- SUCCESS = Green
- FAILED = Red

---

## 🎨 **Layout Suggestion**

### **Page 1: Seismic Analysis**
```
┌─────────────────────────────────────────────────────┐
│  [166K Total] [884 Last 30d] [421 Locations] etc   │  ← 5 KPI Cards
├────────────────────┬────────────────────────────────┤
│                    │                                │
│   Map              │  Line Chart                    │
│   (Visual 1)       │  Monthly Trend (Visual 2)      │
│                    │                                │
├────────────────────┼────────────────────────────────┤
│                    │                                │
│   Scatter          │  Stacked Bar                   │
│   Energy/Freq      │  Shallow vs Deep (Visual 4)    │
│   (Visual 3)       │                                │
├────────────────────┴────────────────────────────────┤
│   Column Chart (Magnitude)  │  Bar (Top 10 Regions) │
│   (Visual 5)                │  (Visual 6)           │
└─────────────────────────────┴───────────────────────┘
```

### **Page 2: Pipeline Health**
```
┌─────────────────────────────────────────────────────┐
│           Pipeline Health Dashboard                 │
├─────────────────────────────────────────────────────┤
│   Stacked Column: Pipeline Status Over Time        │
├─────────────────────────────────────────────────────┤
│   Table: Recent Pipeline Executions                │
└─────────────────────────────────────────────────────┘
```

---

## ✅ **Final Checklist**

### **Page 1 (Seismic Analysis):**
- ✅ 5 KPI Cards (already done!)
- ☐ Visual 1: Map
- ☐ Visual 2: Line Chart (trend)
- ☐ Visual 3: Scatter (energy correlation)
- ☐ Visual 4: Stacked Bar (shallow/deep)
- ☐ Visual 5: Column Chart (magnitude)
- ☐ Visual 6: Bar Chart (top regions)

### **Page 2 (Pipeline Health):**
- ☐ Audit Visual 1: Execution table
- ☐ Audit Visual 2: Status chart

### **Model Configuration:**
- ☐ All 8 relationships created (Many-to-One)
- ☐ Date table marked
- ☐ Sort columns configured

---

## 🎯 **How Each Visual Answers Your Business Questions**

| Question | Visual(s) |
|----------|-----------|
| Q1: Which regions show accelerating activity? | Visual 2 (trend), Visual 6 (top regions) |
| Q2: Shallow to deep ratio by region? | Visual 4 (stacked bar) + Shallow/Deep Ratio KPI |
| Q3: Energy vs frequency correlation? | Visual 3 (scatter with trend line) |
| Q4: Increasing magnitude trends? | Visual 5 (magnitude distribution), Map legend |

---

## 💡 **Quick Tips**

1. **Add Slicers** for interactivity:
   - Date range slicer (`gold_dim_date[full_date]`)
   - Location type slicer
   
2. **Format Theme:**
   - View → Themes → Choose "Executive" or "Solar"

3. **Title Your Dashboard:**
   - Insert → Text box → "Seismo: Global Earthquake Analytics Dashboard"

4. **Save Often:**
   - File → Save → `Seismo_Phase3_Dashboard.pbix`

---

## 📸 **Screenshot for Report**

After completing:
1. Screenshot the full Page 1 dashboard
2. Screenshot Model view showing relationships
3. Screenshot Page 2 (audit page)
4. Include these in your Phase 3 report

---

**You already have 5 KPIs done - just 6 more visuals and you're finished!** 🎉
