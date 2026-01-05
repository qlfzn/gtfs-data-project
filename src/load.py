"""
Load raw data into DuckDB storage.
"""
import duckdb
import os

# init duckdb

# create duckdb storage

# define and create tables

# run queries based on metrics
class DBStorage:
    def __init__(self) -> None:
        DB_PATH = "data/analytics/gtfs.db"
        self.db = duckdb.connect(DB_PATH)

        self.tables = []
    
    def read_files(self, files_dir: str):
        # check if dir exists
        os.makedirs(files_dir, exist_ok=True) 

        for file in os.listdir(files_dir):
            table_name = file.split(".")[0]
            self.db.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{files_dir}/{file}')")
            self.tables.append(table_name)
            print(f"successfully created table: {table_name}")