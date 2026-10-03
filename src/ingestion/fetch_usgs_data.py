"""
USGS Earthquake Data Ingestion Script (v2)
===========================================
Fetches seismic event data from USGS FDSN Event Web Service API
and saves it as GeoJSON files for processing in Databricks.

Generates:
1. Full load (~200 MB, 6 years) - for Databricks upload
2. Sample load (~15 MB, 1 month) - for GitHub
3. Incremental load (~1 MB, 7 days) - for daily updates

Author: FAST-NUCES DS-3001 Team
Project: Automated USGS Seismic Event Telemetry Pipeline
"""

import requests
import json
from datetime import datetime, timedelta
import os
import sys
import time


class USGSDataFetcher:
    """Handles fetching earthquake data from USGS API"""
    
    BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    API_LIMIT = 20000  # USGS API maximum results per query
    
    def __init__(self, output_dir=None):
        """
        Initialize the data fetcher
        
        Args:
            output_dir: Directory where JSON files will be saved (defaults to project's data/samples)
        """
        if output_dir is None:
            # Get the project root (2 levels up from this script)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(script_dir))
            self.output_dir = os.path.join(project_root, "data", "samples")
        else:
            self.output_dir = output_dir
        self._ensure_output_directory()
    
    def _ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        os.makedirs(self.output_dir, exist_ok=True)
    
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
        
        response = requests.get(self.BASE_URL, params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    
    def fetch_full_load(self, start_year, end_year, min_magnitude=2.5,
                       output_filename="usgs_earthquake_full_load.json"):
        """
        Fetch multi-year historical data in monthly batches
        
        Args:
            start_year: Start year (e.g., 2019)
            end_year: End year (e.g., 2025)
            min_magnitude: Minimum earthquake magnitude
            output_filename: Output filename
        
        Returns:
            dict: Combined GeoJSON data
        """
        print(f"\n{'='*60}")
        print(f"FULL LOAD: Multi-Year Historical Data")
        print(f"Period: {start_year}-{end_year}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"{'='*60}\n")
        
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
                    
                    # Stop if we've reached the end year + 1
                    if next_year > end_year:
                        break
                    
                    end_date = f"{next_year}-{next_month:02d}-01"
                    
                    batch_num += 1
                    print(f"Batch {batch_num}: {start_date} to {end_date}...", end=" ")
                    
                    data = self._fetch_batch(start_date, end_date, min_magnitude)
                    batch_count = len(data.get('features', []))
                    all_features.extend(data['features'])
                    total_events += batch_count
                    
                    print(f"✓ {batch_count:,} events")
                    
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
            
            # Save to file
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, indent=2, ensure_ascii=False)
            
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            
            print(f"\n{'='*60}")
            print(f"✓ Full Load Complete!")
            print(f"  • Total events: {total_events:,}")
            print(f"  • File size: {file_size_mb:.2f} MB")
            print(f"  • Batches processed: {batch_num}")
            print(f"  • Saved to: {output_path}")
            print(f"{'='*60}")
            
            return combined_data
            
        except requests.exceptions.RequestException as e:
            print(f"\n✗ Error fetching data: {e}", file=sys.stderr)
            raise
    
    def fetch_sample(self, start_date, end_date, min_magnitude=2.5,
                    output_filename="usgs_earthquake_sample.json"):
        """
        Fetch a smaller sample for GitHub repository
        
        Args:
            start_date: Start date
            end_date: End date
            min_magnitude: Minimum magnitude
            output_filename: Output filename
        
        Returns:
            dict: API response data
        """
        print(f"\n{'='*60}")
        print(f"SAMPLE LOAD: For GitHub Repository")
        print(f"Period: {start_date} to {end_date}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"{'='*60}\n")
        
        try:
            data = self._fetch_batch(start_date, end_date, min_magnitude)
            
            # Add metadata
            data['metadata']['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['load_type'] = 'sample'
            data['metadata']['source'] = 'USGS FDSN Event Web Service'
            data['metadata']['min_magnitude'] = min_magnitude
            
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
    
    def fetch_incremental_load(self, days_back=7, min_magnitude=2.5,
                               output_filename="usgs_earthquake_incremental.json"):
        """
        Fetch incremental updates (new events, updates, and deletions)
        
        Args:
            days_back: Number of days to look back (default: 7 to reach 1+ MB)
            min_magnitude: Minimum earthquake magnitude (MUST match full load)
            output_filename: Name of output JSON file
        
        Returns:
            dict: API response data
        """
        updated_after = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
        
        print(f"\n{'='*60}")
        print(f"INCREMENTAL LOAD: Updates & Deletions")
        print(f"Updated after: {updated_after}")
        print(f"Days back: {days_back}")
        print(f"Minimum Magnitude: {min_magnitude}")
        print(f"Include deleted: Yes")
        print(f"{'='*60}\n")
        
        params = {
            'format': 'geojson',
            'updatedafter': updated_after,
            'minmagnitude': min_magnitude,
            'includedeleted': 'true',  # Critical for soft-delete logic
            'orderby': 'time'
        }
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            
            # Add ingestion metadata
            data['metadata']['ingestion_timestamp'] = datetime.utcnow().isoformat()
            data['metadata']['load_type'] = 'incremental_load'
            data['metadata']['updated_after'] = updated_after
            data['metadata']['days_back'] = days_back
            data['metadata']['source'] = 'USGS FDSN Event Web Service'
            data['metadata']['min_magnitude'] = min_magnitude
            data['metadata']['includes_deleted'] = True
            
            # Save to file
            output_path = os.path.join(self.output_dir, output_filename)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            event_count = len(data.get('features', []))
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            
            # Count deleted events
            deleted_count = sum(1 for f in data.get('features', []) 
                              if f.get('properties', {}).get('status') == 'deleted')
            
            print(f"✓ Success!")
            print(f"  • Events fetched: {event_count:,}")
            print(f"  • Deleted events: {deleted_count}")
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
        print(f"Felt Reports: {props.get('felt', 'None')}")


def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("USGS EARTHQUAKE DATA INGESTION (V2)")
    print("DS-3001 Data Engineering Project - Phase 1")
    print("="*60)
    
    # Initialize fetcher (will auto-detect project root)
    fetcher = USGSDataFetcher()
    
    # 1. FULL LOAD: 6 years (2019-2025) - Target ~200 MB
    #    Magnitude 2.5+ to get sufficient volume
    print("\n" + "="*60)
    print("STEP 1: Full Load (for Databricks)")
    print("="*60)
    try:
        full_data = fetcher.fetch_full_load(
            start_year=2019,
            end_year=2024,  # Up to 2024-12-31
            min_magnitude=2.5,
            output_filename="usgs_earthquake_full_load.json"
        )
        fetcher.get_sample_event_details(full_data)
    except Exception as e:
        print(f"Full load failed: {e}", file=sys.stderr)
    
    # 2. SAMPLE LOAD: 1 month - For GitHub (~15 MB)
    print("\n" + "="*60)
    print("STEP 2: Sample Load (for GitHub)")
    print("="*60)
    try:
        sample_data = fetcher.fetch_sample(
            start_date="2024-12-01",
            end_date="2025-01-01",
            min_magnitude=2.5,
            output_filename="usgs_earthquake_sample.json"
        )
        fetcher.get_sample_event_details(sample_data)
    except Exception as e:
        print(f"Sample load failed: {e}", file=sys.stderr)
    
    # 3. INCREMENTAL LOAD: Last 7 days - Target 1+ MB
    #    SAME magnitude filter as full load (per instructor feedback)
    print("\n" + "="*60)
    print("STEP 3: Incremental Load (for daily updates)")
    print("="*60)
    try:
        incremental_data = fetcher.fetch_incremental_load(
            days_back=7,  # 7 days to reach 1+ MB
            min_magnitude=2.5,  # SAME as full load
            output_filename="usgs_earthquake_incremental.json"
        )
        fetcher.get_sample_event_details(incremental_data)
    except Exception as e:
        print(f"Incremental load failed: {e}", file=sys.stderr)
    
    print(f"\n{'='*60}")
    print("✓ Data ingestion complete!")
    print(f"\nFiles generated:")
    print(f"  1. usgs_earthquake_full_load.json (~200 MB) → Upload to Databricks")
    print(f"  2. usgs_earthquake_sample.json (~15 MB) → Commit to GitHub")
    print(f"  3. usgs_earthquake_incremental.json (~1 MB) → Commit to GitHub")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
