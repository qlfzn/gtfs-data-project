import requests
import zipfile
import os
import io       
from datetime import datetime
import boto3

DATA_DIR = "data"
curr_date = datetime.now().date()

class Extractor:
    def __init__(self, bucket_name: str) -> None:
        """
        Initialise S3 client
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )

    def fetch_gtfs_data(self, category: str):
        os.makedirs(DATA_DIR, exist_ok=True)
        
        try:
            url = f"https://api.data.gov.my/gtfs-static/prasarana?category={category}"
            r = requests.get(url)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"Failed to fetch data: {e}")
            raise

    def extract_and_upload_to_s3(self, response):
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file in z.filelist:
                    if file.filename.endswith("/") or "__MACOSX" in file.filename or file.filename.startswith("._"):
                        continue

                    # read content
                    file_content = z.read(file.filename)

                    s3_key = f"raw/{curr_date}/{file.filename}"
                    
                    # reads in-memory
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=file_content
                    )
                    print(f"Uploaded {file.filename} to s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Failed to extract and upload to s3: {e}")