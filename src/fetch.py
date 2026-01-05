import requests
import zipfile
import os
import io       
from datetime import datetime

DATA_DIR = "data"

curr_date = datetime.now().date()

def fetch_gtfs_data(category):
    os.makedirs(DATA_DIR, exist_ok=True)
    
    url = f"https://api.data.gov.my/gtfs-static/prasarana?category={category}"
    r = requests.get(url)
    r.raise_for_status()

    return r

def extract_data(response):
    os.makedirs(DATA_DIR, exist_ok=True)

    z = zipfile.ZipFile(io.BytesIO(response.content))

    z.extractall(f"{DATA_DIR}/{curr_date}/")

def is_new_extraction() -> bool:
    
    return True

response = fetch_gtfs_data(category="rapid-rail-kl")
extract_data(response=response)