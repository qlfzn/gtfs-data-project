"""
Main script for running data extraction from GTFS API -> DuckDB analytics
"""
from src import DBStorage

class GTFSPipeline:
    def __init__(self) -> None:
        self.loader = DBStorage()

    def run_load_tables(self):
        print("Reading and creating tables...")
        
        try:
            self.loader.read_files(files_dir="./data")
        except Exception as e:
            print(f"Failed to load tables: {e}")

if __name__ == "__main__":
    gtfs = GTFSPipeline()
    gtfs.run_load_tables()