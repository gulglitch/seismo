# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_Fetch_USGS_Data — API Data Ingestion to DBFS
# MAGIC
# MAGIC **Purpose:** Fetch earthquake data from USGS API and save directly to DBFS
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `output_path`: DBFS path where JSON files will be saved
# MAGIC - `fetch_type`: FULL_LOAD / SAMPLE / INCREMENTAL_LOAD
# MAGIC - `start_year`: Start year for full load (e.g., 2019)
# MAGIC - `end_year`: End year for full load (e.g., 2024)
# MAGIC - `days_back`: Days to look back for incremental load (default: 7)
# MAGIC - `min_magnitude`: Minimum earthquake magnitude (default: 2.5)
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Connects to USGS FDSN Event Web Service API
# MAGIC 2. Fetches earthquake data based on selected type
# MAGIC 3. Saves GeoJSON directly to DBFS
# MAGIC 4. Validates file creation and size
# MAGIC
# MAGIC **Author:** Seismo Team - Phase 2  
# MAGIC **Last Updated:** October 9, 2026

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration & Widgets

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
import time

# Create widgets
dbutils.widgets.text("output_path", "/Volumes/workspace/seismo/raw/", "Output Path (DBFS)")
dbutils.widgets.dropdown("fetch_type", "FULL_LOAD", ["FULL_LOAD", "SAMPLE", "INCREMENTAL_LOAD"], "Fetch Type")
dbutils.widgets.text("start_year", "2019", "Start Year (Full Load)")
dbutils.widgets.text("end_year", "2024", "End Year (Full Load)")
dbutils.widgets.text("days_back", "7", "Days Back (Incremental)")
dbutils.widgets.text("min_magnitude", "2.5", "Min Magnitude")

print("✅ Widgets created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core API Functions

# COMMAND ----------

class USGSDataFetcher:
    """Handles fetching earthquake data from USGS API"""
    
    BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    API_LIMIT = 20000  # USGS API maximum results per query
    
    def __init__(self):
        """Initialize the data fetcher"""
        pass
    
    def _fetch_batch(self, start_date, end_date, min_magnitude, include_deleted=False):
        """
        Fetch a single batch from the API
        
        Args:
            start_date: Start date in ISO format
            end_date: End date in ISO format
            min_magnitude: Minimum magnitude
            include_deleted: Whether to include deleted events
        
        Returns:
            dict: API response data
        """
        params = {
            'format': 'geojson',
            'starttime': start_date,
            'endtime': end_date,
            'minmagnitude': min_magnitude,
            'limit': self.API_LIMIT,
            'orderby': 'time'
        }
        
        if include_deleted:
            params['includedeleted'] = 'true'
        
        print(f"   Calling API: {start_date} to {end_date}...", end=" ")
        response = requests.get(self.BASE_URL, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        print(f"✓ {len(data.get('features', [])):,} events")
        
        return data
    
    def fetch_full_load(self, start_year, end_year, min_magnitude):
        """
        Fetch multi-year historical data in monthly batches
        
        Args:
            start_year: Start year (e.g., 2019)
            end_year: End year (e.g., 2024)
            min_magnitude: Minimum earthquake magnitude
        
        Returns:
            dict: Combined GeoJSON data
        """
        print(f"\n{'='*70}")
        print(f"FULL LOAD: Multi-Year Historical Data")
        print(f"Period: {start_year}-{end_year}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"{'='*70}\n")
        
        all_features = []
        total_events = 0
        batch_num = 0
        
        try:
            # Generate monthly batches for the entire period
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    # Calculate start and end dates
                    start_date = f"{year}-{month:02d}-01"
                    
                    # Calculate next month for end date
                    next_month = month + 1
                    next_year = year
                    if next_month > 12:
                        next_month = 1
                        next_year = year + 1
                    
                    # Stop if we've reached beyond end year
                    if next_year > end_year or (next_year == end_year and next_month > 12):
                        break
                    
                    end_date = f"{next_year}-{next_month:02d}-01"
                    
                    batch_num += 1
                    print(f"Batch {batch_num}: ", end="")
                    
                    data = self._fetch_batch(start_date, end_date, min_magnitude)
                    batch_count = len(data.get('features', []))
                    all_features.extend(data['features'])
                    total_events += batch_count
                    
                    # Small delay to be respectful to API
                    time.sleep(0.5)
            
            # Create combined GeoJSON structure
            combined_data = {
                'type': 'FeatureCollection',
                'metadata': {
                    'generated': int(datetime.utcnow().timestamp() * 1000),
                    'title': f'USGS Earthquakes {start_year}-{end_year}',
                    'count': total_events,
                    'ingestion_timestamp': datetime.utcnow().isoformat(),
                    'load_type': 'full_load',
                    'source': 'USGS FDSN Event Web Service',
                    'min_magnitude': min_magnitude,
                    'batches': batch_num,
                    'period': f'{start_year}-{end_year}'
                },
                'features': all_features
            }
            
            print(f"\n{'='*70}")
            print(f"✅ Full Load Complete!")
            print(f"   • Total events: {total_events:,}")
            print(f"   • Batches processed: {batch_num}")
            print(f"{'='*70}\n")
            
            return combined_data
            
        except requests.exceptions.RequestException as e:
            print(f"\n❌ Error fetching data: {e}")
            raise
    
    def fetch_sample(self, start_date, end_date, min_magnitude):
        """
        Fetch a smaller sample dataset
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            min_magnitude: Minimum magnitude
        
        Returns:
            dict: API response data
        """
        print(f"\n{'='*70}")
        print(f"SAMPLE LOAD: For Testing")
        print(f"Period: {start_date} to {end_date}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"{'='*70}\n")
        
        try:
            data = self._fetch_batch(start_date, end_date, min_magnitude)
            
            # Add metadata
            data['metadata']['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['load_type'] = 'sample'
            data['metadata']['source'] = 'USGS FDSN Event Web Service'
            data['metadata']['min_magnitude'] = min_magnitude
            
            event_count = len(data.get('features', []))
            
            print(f"\n✅ Sample Load Complete!")
            print(f"   • Events fetched: {event_count:,}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data: {e}")
            raise
    
    def fetch_incremental_load(self, days_back, min_magnitude):
        """
        Fetch incremental updates (new events, updates, and deletions)
        
        Args:
            days_back: Number of days to look back
            min_magnitude: Minimum earthquake magnitude
        
        Returns:
            dict: API response data
        """
        updated_after = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
        
        print(f"\n{'='*70}")
        print(f"INCREMENTAL LOAD: Updates & Deletions")
        print(f"Updated after: {updated_after}")
        print(f"Days back: {days_back}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"Include deleted: Yes")
        print(f"{'='*70}\n")
        
        params = {
            'format': 'geojson',
            'updatedafter': updated_after,
            'minmagnitude': min_magnitude,
            'includedeleted': 'true',
            'orderby': 'time'
        }
        
        try:
            print(f"   Calling API with updatedafter parameter...", end=" ")
            response = requests.get(self.BASE_URL, params=params, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            event_count = len(data.get('features', []))
            print(f"✓ {event_count:,} events")
            
            # Add ingestion metadata
            data['metadata']['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['load_type'] = 'incremental_load'
            data['metadata']['updated_after'] = updated_after
            data['metadata']['days_back'] = days_back
            data['metadata']['source'] = 'USGS FDSN Event Web Service'
            data['metadata']['min_magnitude'] = min_magnitude
            data['metadata']['includes_deleted'] = True
            
            # Count deleted events
            deleted_count = sum(1 for f in data.get('features', []) 
                              if f.get('properties', {}).get('status') == 'deleted')
            
            print(f"\n✅ Incremental Load Complete!")
            print(f"   • Events fetched: {event_count:,}")
            print(f"   • Deleted events: {deleted_count}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"\n❌ Error fetching data: {e}")
            raise
    
    def display_sample_event(self, data):
        """
        Display sample event details for verification
        
        Args:
            data: GeoJSON response data
        """
        if not data.get('features'):
            print("⚠️  No events found in dataset")
            return
        
        print(f"\n{'='*70}")
        print("SAMPLE EVENT DETAILS (First Event)")
        print(f"{'='*70}\n")
        
        sample = data['features'][0]
        props = sample['properties']
        coords = sample['geometry']['coordinates']
        
        print(f"Event ID:        {sample['id']}")
        print(f"Magnitude:       {props.get('mag')} {props.get('magType', 'N/A')}")
        print(f"Location:        {props.get('place', 'Unknown')}")
        print(f"Depth:           {coords[2]:.2f} km")
        print(f"Coordinates:     [{coords[1]:.4f}, {coords[0]:.4f}]")
        print(f"Time:            {datetime.fromtimestamp(props['time']/1000).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"Status:          {props.get('status', 'N/A')}")
        print(f"Felt Reports:    {props.get('felt', 'None')}")
        print(f"{'='*70}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Fetch

# COMMAND ----------

# Parse widgets
output_path = dbutils.widgets.get("output_path")
fetch_type = dbutils.widgets.get("fetch_type")
start_year = int(dbutils.widgets.get("start_year"))
end_year = int(dbutils.widgets.get("end_year"))
days_back = int(dbutils.widgets.get("days_back"))
min_magnitude = float(dbutils.widgets.get("min_magnitude"))

print(f"{'='*70}")
print("USGS EARTHQUAKE DATA INGESTION")
print(f"{'='*70}")
print(f"Fetch Type:      {fetch_type}")
print(f"Output Path:     {output_path}")
print(f"Min Magnitude:   {min_magnitude}")
print(f"{'='*70}\n")

# Initialize fetcher
fetcher = USGSDataFetcher()

# Fetch data based on type
if fetch_type == "FULL_LOAD":
    data = fetcher.fetch_full_load(start_year, end_year, min_magnitude)
    filename = f"usgs_earthquake_full_load_{start_year}_{end_year}.json"
    
elif fetch_type == "SAMPLE":
    # Sample: 1 month of recent data
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    data = fetcher.fetch_sample(start_date, end_date, min_magnitude)
    filename = f"usgs_earthquake_sample.json"
    
elif fetch_type == "INCREMENTAL_LOAD":
    data = fetcher.fetch_incremental_load(days_back, min_magnitude)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"usgs_earthquake_incremental_{timestamp}.json"

# Display sample event
fetcher.display_sample_event(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to DBFS

# COMMAND ----------

# Ensure output path exists
try:
    dbutils.fs.mkdirs(output_path)
    print(f"✅ Output directory ready: {output_path}")
except:
    print(f"ℹ️  Output directory already exists: {output_path}")

# Convert data to JSON string
json_string = json.dumps(data, indent=2, ensure_ascii=False)
data_size_mb = len(json_string.encode('utf-8')) / (1024 * 1024)

# Full DBFS path
full_path = f"{output_path.rstrip('/')}/{filename}"

print(f"\n{'='*70}")
print(f"💾 Saving to DBFS...")
print(f"   Path: {full_path}")
print(f"   Size: {data_size_mb:.2f} MB")
print(f"{'='*70}\n")

# Save to DBFS
dbutils.fs.put(full_path, json_string, overwrite=True)

print(f"✅ File saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Saved File

# COMMAND ----------

# List files in output directory
print(f"📁 Files in {output_path}:\n")
files = dbutils.fs.ls(output_path)
display(files)

# COMMAND ----------

# Verify file exists and check size
try:
    file_info = dbutils.fs.ls(full_path)[0]
    saved_size_mb = file_info.size / (1024 * 1024)
    
    print(f"\n{'='*70}")
    print(f"✅ FILE VERIFICATION SUCCESSFUL")
    print(f"{'='*70}")
    print(f"File:            {filename}")
    print(f"Path:            {full_path}")
    print(f"Size:            {saved_size_mb:.2f} MB")
    print(f"Events:          {data['metadata']['count']:,}")
    print(f"Load Type:       {data['metadata']['load_type']}")
    print(f"Timestamp:       {data['metadata']['ingestion_timestamp']}")
    print(f"{'='*70}\n")
    
except Exception as e:
    print(f"❌ File verification failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Next Steps

# COMMAND ----------

print(f"\n{'='*70}")
print("🎉 DATA INGESTION COMPLETE!")
print(f"{'='*70}\n")

print(f"✅ What was done:")
print(f"   • Fetched {data['metadata']['count']:,} earthquake events from USGS API")
print(f"   • Saved {data_size_mb:.2f} MB GeoJSON file to DBFS")
print(f"   • File location: {full_path}")

print(f"\n📋 Next Steps:")
if fetch_type == "FULL_LOAD":
    print(f"   1. Run 01_Bronze_Ingestion.py with:")
    print(f"      source_path = {full_path}")
    print(f"      ingestion_type = FULL_LOAD")
elif fetch_type == "INCREMENTAL_LOAD":
    print(f"   1. Run 01_Bronze_Ingestion.py with:")
    print(f"      source_path = {full_path}")
    print(f"      ingestion_type = INCREMENTAL_LOAD")
else:
    print(f"   1. Use this file for testing Bronze ingestion")

print(f"\n💡 Tips:")
print(f"   • Copy the full path above for the next notebook")
print(f"   • Verify data quality by checking sample events")
print(f"   • For incremental loads, run this notebook daily/weekly")

print(f"\n{'='*70}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Quick Data Preview (Optional)

# COMMAND ----------

# Read the saved file back and preview with Spark
df_preview = spark.read.option("multiLine", "true").json(full_path)
print(f"\n📊 Data Schema Preview:")
df_preview.printSchema()

print(f"\n📋 First 5 Features:")
display(df_preview.select("features").limit(1))