"""
Load raw data into DuckDB storage.
"""
import duckdb
import boto3
import os
from datetime import datetime

class LoadData:
    def __init__(self, bucket_name: str) -> None:
        """
        Initialize DuckDB and S3 client
        """
        DB_PATH = "data/analytics/gtfs_raw.db"
        self.db = duckdb.connect(DB_PATH)
        self.db.execute("INSTALL httpfs;")
        self.db.execute("LOAD httpfs;")

        self.db.execute("""
            CREATE SECRET (
                TYPE S3,
                KEY_ID 'minioadmin',
                SECRET 'minioadmin',
                ENDPOINT 'localhost:9000',
                USE_SSL false
            )
        """)

        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )

        self.bucket_name = bucket_name
        self.tables = []

    def read_file(self, date_str: str):
        if date_str is None:
            date_str = datetime.now().date().isoformat()
        
        s3_prefix = f"raw/{date_str}/"
        try:
            # list all files based on date
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix
            )
            
            if 'Contents' not in response:
                print(f"No files found in s3://{self.bucket_name}/{s3_prefix}")
                return
            
            for obj in response['Contents']:
                key = obj['Key']
                filename = os.path.basename(key)
                
                if not filename or filename.endswith('/'):
                    continue
                
                table_name = filename.split(".")[0]
                
                self.db.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto(
                        's3://{self.bucket_name}/{key}'
                    )
                """)
                
                self.tables.append(table_name)
                print(f"Loaded {table_name} from {key}")
        
        except Exception as e:
            print(f"Failed to read files from S3: {e}")
            raise

    def append_rows_to_table(self, table_name: str, date_str: str):
        if date_str is None:
            date_str = datetime.now().date().isoformat()
        
        s3_key = f"gtfs/{date_str}/{table_name}.txt"
        
        try:
            result = self.db.execute(
                f"SELECT name FROM information_schema.tables WHERE table_name='{table_name}'"
            ).fetchall()
            
            if not result:
                print(f"Table {table_name} doesn't exist. Creating new table...")
                self.db.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto(
                        's3://{self.bucket_name}/{s3_key}'
                    )
                """)
            else:
                self.db.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_csv_auto(
                        's3://{self.bucket_name}/{s3_key}'
                    )
                """)
                print(f"Appended data to {table_name}")
        
        except Exception as e:
            print(f"Failed to append rows to {table_name}: {e}")
            raise