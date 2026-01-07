"""
Main script for running data extraction from GTFS API -> DuckDB analytics
"""
from src import Extractor
from src import LoadData

from datetime import datetime
class GTFSPipeline:
    def __init__(self) -> None:
        self.extract = Extractor(bucket_name="gtfs-data")
        self.loader = LoadData(bucket_name='gtfs-data')

    def run_extract_data(self):
        try:
            response = self.extract.fetch_gtfs_data(category="rapid-rail-kl")
            self.extract.extract_and_upload_to_s3(response=response)
            print("Successfully fetched data from API")
        except Exception as e:
            print(f"Failed to fetch data: {e}")

    def run_load_tables(self):
        print("Reading and creating tables...")
        
        try:
            self.loader.read_file(date_str=str(datetime.now().date()))
        except Exception as e:
            print(f"Failed to load tables: {e}")

if __name__ == "__main__":
    gtfs = GTFSPipeline()
    # gtfs.run_extract_data()
    gtfs.run_load_tables()