# How to Upload and Run in Databricks

## 📋 Quick Start Guide

### Step 1: Upload Notebook to Databricks

1. **Log in to Databricks**
2. **Go to Workspace** → Your folder
3. **Click Import**
4. Select `00_Fetch_USGS_Data.py`
5. Click **Import**

---

### Step 2: Configure Output Path

Before running, you need to set the correct DBFS path:

**Option A: Using Volumes (Recommended if available)**
```
/Volumes/<catalog>/<schema>/raw/
```
Example: `/Volumes/main/seismo/raw/`

**Option B: Using DBFS (Standard)**
```
/FileStore/seismo_data/raw/
```

**Option C: Using dbfs:/ prefix**
```
dbfs:/FileStore/seismo_data/raw/
```

---

### Step 3: Run the Notebook

#### For FULL LOAD (Historical Data):
1. Set widgets:
   - `output_path`: `/FileStore/seismo_data/raw/`
   - `fetch_type`: **FULL_LOAD**
   - `start_year`: **2019**
   - `end_year`: **2024**
   - `min_magnitude`: **2.5**

2. Click **Run All**

3. Wait ~10-15 minutes (fetching 6 years of data in monthly batches)

4. Expected output: `usgs_earthquake_full_load_2019_2024.json` (~100-200 MB)

---

#### For SAMPLE (Testing):
1. Set widgets:
   - `output_path`: `/FileStore/seismo_data/raw/`
   - `fetch_type`: **SAMPLE**
   - `min_magnitude`: **2.5**

2. Click **Run All**

3. Takes ~30 seconds

4. Expected output: `usgs_earthquake_sample.json` (~10-20 MB)

---

#### For INCREMENTAL LOAD (Updates):
1. Set widgets:
   - `output_path`: `/FileStore/seismo_data/raw/`
   - `fetch_type`: **INCREMENTAL_LOAD**
   - `days_back`: **7**
   - `min_magnitude`: **2.5**

2. Click **Run All**

3. Takes ~30 seconds

4. Expected output: `usgs_earthquake_incremental_<timestamp>.json` (~1-5 MB)

---

## 🔍 Troubleshooting

### Error: "Path does not exist"
**Solution**: The output path might not exist yet. The notebook creates it automatically, but if you get this error:
```python
dbutils.fs.mkdirs("/FileStore/seismo_data/raw/")
```

### Error: "API request timeout"
**Solution**: 
- Check your cluster has internet access
- Try running with `fetch_type = SAMPLE` first
- If blocked, contact TA (you can use manual upload per announcement)

### Error: "Too many requests"
**Solution**: The USGS API has rate limits
- The notebook includes 0.5s delays between batches
- If you hit limits, wait 5 minutes and retry

---

## 📊 After Successful Run

### Verify the File:
```python
# Run this in a new cell
display(dbutils.fs.ls("/FileStore/seismo_data/raw/"))
```

### Copy the Path for Next Step:
The notebook will output something like:
```
File location: /FileStore/seismo_data/raw/usgs_earthquake_full_load_2019_2024.json
```

**Copy this path!** You'll need it for `01_Bronze_Ingestion.py`

---

## 🚀 Next Steps

### After data is in DBFS:

1. **Run Bronze Ingestion**:
   ```python
   # In 01_Bronze_Ingestion.py widgets:
   source_path = "/FileStore/seismo_data/raw/usgs_earthquake_full_load_2019_2024.json"
   ingestion_type = "FULL_LOAD"
   batch_id = "FULL_2026-10-09"  # or leave blank for auto-generation
   ```

2. **Run Silver Transformation**:
   ```python
   # After Bronze completes successfully
   # Run 02_Silver_transformations.py
   ```

---

## 💡 Pro Tips

### For Evaluation:
- Run **SAMPLE** first to test the pipeline quickly
- Then run **FULL_LOAD** for complete dataset
- Keep screenshots of successful runs

### Widget Values to Remember:
Always use **same min_magnitude (2.5)** across all loads to maintain consistency

### Time Estimates:
- **SAMPLE**: ~30 seconds
- **INCREMENTAL**: ~30-60 seconds  
- **FULL_LOAD (2019-2024)**: ~10-15 minutes (72 monthly batches)

---

## ❓ Questions?

### "Should I run this or just use my uploaded files?"
**Answer**: Run this in Databricks to show complete API → DBFS flow. This meets the requirement "API call should be in Databricks."

### "What if USGS blocks my API?"
**Answer**: Per TA announcement, manual CSV upload is acceptable. But try the notebook first - it's respectful to API with delays.

### "Can I reduce the full load size?"
**Answer**: Yes! Change `start_year = 2022` to get ~3 years instead of 6. Still meets >100MB requirement.

---

## ✅ Checklist

Before moving to Bronze layer:

- [ ] Notebook uploaded to Databricks
- [ ] Output path configured correctly
- [ ] At least one successful run (SAMPLE or FULL_LOAD)
- [ ] File verified in DBFS with `dbutils.fs.ls()`
- [ ] File path copied for Bronze ingestion
- [ ] Screenshot taken for documentation

Good luck! 🎉
