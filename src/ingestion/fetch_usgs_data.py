"""
USGS Earthquake Data Ingestion Script
======================================
Fetches seismic event data from USGS FDSN Event Web Service API
and saves it as GeoJSON files for processing in Databricks.

Author: FAST-NUCES DS-3001 Team
Project: Automated USGS Seismic Event Telemetry Pipeline
"""

import requests
import json
from datetime import datetime, timedelta
import os
import sys


class USGSDataFetcher:
    """Handles fetching earthquake data from USGS API"""
    
    BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    
    def __init__(self, output_dir="../../data/samples"):
        """
        Initialize the data fetcher
        
        Args:
            output_dir: Directory where JSON files will be saved
        """
        self.output_dir = output_dir
        self._ensure_output_directory()
    
    def _ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        os.makedirs(self.output_dir, exist_ok=True)
    
    def fetch_full_load(self, start_date, end_date, min_magnitude=2.5, 
                       output_filename="usgs_earthquake_full_load.json"):
        """
        Fetch historical baseline data (Full Load)
        
        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            min_magnitude: Minimum earthquake magnitude to fetch
            output_filename: Name of output JSON file
        
        Returns:
            dict: API response data
        """
        print(f"\n{'='*60}")
        print(f"FULL LOAD: Fetching historical data")
        print(f"Period: {start_date} to {end_date}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"{'='*60}\n")
        
        params = {
            'format': 'geojson',
            'starttime': start_date,
            'endtime': end_date,
            'minmagnitude': min_magnitude,
            'orderby': 'time'
        }
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Add ingestion metadata
            data['metadata']['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['load_type'] = 'full_load'
            data['metadata']['source'] = 'USGS FDSN Event Web Service'
            
            # Save to file
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            event_count = len(data.get('features', []))
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            
            print(f"✓ Success!")
            print(f"  • Events fetched: {event_count:,}")
            print(f"  • File size: {file_size_mb:.2f} MB")
            print(f"  • Saved to: {output_path}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching data: {e}", file=sys.stderr)
            raise
    
    def fetch_incremental_load(self, updated_after=None, 
                               output_filename="usgs_earthquake_incremental.json"):
        """
        Fetch incremental updates (new events, updates, and deletions)
        
        Args:
            updated_after: ISO timestamp for incremental updates (defaults to last 24 hours)
            output_filename: Name of output JSON file
        
        Returns:
            dict: API response data
        """
        if updated_after is None:
            # Default: fetch events updated in last 24 hours
            updated_after = (datetime.utcnow() - timedelta(days=1)).isoformat()
        
        print(f"\n{'='*60}")
        print(f"INCREMENTAL LOAD: Fetching updates")
        print(f"Updated after: {updated_after}")
        print(f"{'='*60}\n")
        
        params = {
            'format': 'geojson',
            'updatedafter': updated_after,
            'orderby': 'time'
        }
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Add ingestion metadata
            data['metadata']['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['load_type'] = 'incremental_load'
            data['metadata']['updated_after'] = updated_after
            data['metadata']['source'] = 'USGS FDSN Event Web Service'
            
            # Save to file
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            event_count = len(data.get('features', []))
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            
            print(f"✓ Success!")
            print(f"  • Events fetched: {event_count:,}")
            print(f"  • File size: {file_size_mb:.2f} MB")
            print(f"  • Saved to: {output_path}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching data: {e}", file=sys.stderr)
            raise
    
    def get_sample_event_details(self, data):
        """
        Extract and display sample event details for verification
        
        Args:
            data: GeoJSON response data
        """
        if not data.get('features'):
            print("No events found in dataset")
            return
        
        print(f"\n{'='*60}")
        print("SAMPLE EVENT DETAILS (First Event)")
        print(f"{'='*60}\n")
        
        sample = data['features'][0]
        props = sample['properties']
        coords = sample['geometry']['coordinates']
        
        print(f"Event ID:     {sample['id']}")
        print(f"Magnitude:    {props.get('mag')} {props.get('magType', 'N/A')}")
        print(f"Location:     {props.get('place', 'Unknown')}")
        print(f"Depth:        {coords[2]:.2f} km")
        print(f"Coordinates:  [{coords[1]:.4f}, {coords[0]:.4f}]")
        print(f"Time:         {datetime.fromtimestamp(props['time']/1000).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"Status:       {props.get('status', 'N/A')}")
        print(f"Felt Reports: {props.get('felt', 0)}")


def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("USGS EARTHQUAKE DATA INGESTION")
    print("DS-3001 Data Engineering Project - Phase 1")
    print("="*60)
    
    # Initialize fetcher
    fetcher = USGSDataFetcher(output_dir="../../data/samples")
    
    # FULL LOAD: 2-year historical baseline (2023-2025)
    # Using magnitude >= 4.5 to get manageable dataset size
    try:
        full_data = fetcher.fetch_full_load(
            start_date="2023-01-01",
            end_date="2025-01-01",
            min_magnitude=4.5,
            output_filename="usgs_earthquake_full_load.json"
        )
        fetcher.get_sample_event_details(full_data)
    except Exception as e:
        print(f"Full load failed: {e}", file=sys.stderr)
    
    # INCREMENTAL LOAD: Last 24 hours
    try:
        incremental_data = fetcher.fetch_incremental_load(
            output_filename="usgs_earthquake_incremental.json"
        )
        if incremental_data.get('features'):
            fetcher.get_sample_event_details(incremental_data)
    except Exception as e:
        print(f"Incremental load failed: {e}", file=sys.stderr)
    
    print(f"\n{'='*60}")
    print("✓ Data ingestion complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
