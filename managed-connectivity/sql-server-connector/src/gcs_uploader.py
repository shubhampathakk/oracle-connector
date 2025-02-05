"""Sends files to GCP storage."""
from typing import Dict
from google.cloud import storage


def upload(config: Dict[str, str], filename: str, folder: str):
    """Uploads a file to GCP bucket."""
    client = storage.Client()
    bucket = client.get_bucket(config["output_bucket"])
    folder = config["output_folder"]

    blob = bucket.blob(f"{folder}/{filename}")
    print(f"Uploading to {folder}/{filename}...")
    blob.upload_from_filename(filename)

def checkDestination(config: Dict[str, str]):
    """Check GCS output folder exists"""
    client = storage.Client()
    bucketpath = config["output_bucket"]
    checkpath = bucketpath 
    bucket = client.bucket(checkpath)

    if not bucket.exists():
        print(f"Output cloud storage bucket {checkpath} does not exist")
        return False
    
    return True
