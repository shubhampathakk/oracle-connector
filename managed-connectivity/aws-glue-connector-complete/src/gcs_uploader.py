# """Sends files to GCP storage."""
# from typing import Dict
# from google.cloud import storage


# def upload(config: Dict[str, str], filename: str, folder: str):
#     """Uploads a file to GCP bucket."""
#     client = storage.Client()
#     bucket = client.get_bucket(config["output_bucket"])
#    # folder = config["output_folder"]

#     blob = bucket.blob(f"{folder}/{filename}")
#     blob.upload_from_filename(filename)

# def checkDestination(config: Dict[str, str]):
#     """Check GCS output folder exists"""
#     client = storage.Client()
#     bucketpath = config["output_bucket"]
#     checkpath = bucketpath 
#     bucket = client.bucket(checkpath)

#     if not bucket.exists():
#         print(f"Output cloud storage bucket {checkpath} does not exist")
#         return False
    
#     return True

import json
from google.cloud import storage

class GCSUploader:
    def __init__(self, project_id, bucket_name):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.get_bucket(bucket_name)

    def upload_entries(self, entries, destination_blob_name="aws-glue-output.jsonl"):
        """Uploads a list of dictionaries as a JSONL file to GCS."""
        
        # Convert list of dicts to a JSONL formatted string
        jsonl_data = "\n".join(json.dumps(entry) for entry in entries)
        
        # Create a new blob and upload the data
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(jsonl_data)

        print(f"Data uploaded to gs://{self.bucket_name}/{destination_blob_name}")