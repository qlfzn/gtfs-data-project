import requests
import zipfile
import os
import io       

DATA_DIR = "data"

def fetch_gtfs_data(category):
    os.makedirs(DATA_DIR, exist_ok=True)
    
    url = f"https://api.data.gov.my/gtfs-static/prasarana?category={category}"
    r = requests.get(url)
    r.raise_for_status()

def extract_data(response):
    os.makedirs(DATA_DIR, exist_ok=True)

    z = zipfile.ZipFile(io.BytesIO(response.content))

    z.extractall(DATA_DIR)

fetch_gtfs_data(category="rapid-rail-kl")