"""The entrypoint of AWS Glue connector pipeline."""
from typing import Dict
import sys
import json
from datetime import datetime

from src.constants import EntryType, SOURCE_TYPE
from src import cmd_reader, secret_manager, entry_builder, gcs_uploader
from src.aws_glue_connector import AWSGlueConnector

def run():
    """Runs the AWS Glue connector pipeline."""
    config = cmd_reader.read_args()

    if not gcs_uploader.checkDestination(config):
        print("Exiting")
        sys.exit()

    # Get AWS credentials from Secret Manager
    try:
    # Get AWS credentials from Secret Manager
        aws_access_key, aws_secret_key = secret_manager.SecretManager.get_aws_credentials(
            config["target_project_id"], 
            config["aws_credentials_secret"]  # You'll need to add this parameter
        )
        config["aws_access_key_id"] = aws_access_key
        config["aws_secret_access_key"] = aws_secret_key
    except Exception as ex:
        print(ex)
        print("Exiting")
        sys.exit()


    # Build output filename and folder
    currentDate = datetime.now()
    FOLDERNAME = f"{SOURCE_TYPE}/{currentDate.year}{currentDate.month}{currentDate.day}-{currentDate.hour}{currentDate.minute}{currentDate.second}"
    FILENAME = f"aws-glue-output-{config['aws_region']}.jsonl"

    print(f"Output folder: {config['output_bucket']}/{FOLDERNAME}")

    connector = AWSGlueConnector(config)
    
    with open(FILENAME, "w", encoding="utf-8") as file:
        # Build catalog entry
        catalog_entry = entry_builder.build_catalog_entry(config)
        file.write(json.dumps(catalog_entry) + "\n")
        
        # Get and process databases
        databases = connector.get_databases()
        print(f"Found {len(databases)} databases")
        
        database_entries = entry_builder.build_database_entries(config, databases)
        for entry in database_entries:
            file.write(json.dumps(entry) + "\n")
        
        # Get and process tables for each database
        total_tables = 0
        for db in databases:
            db_name = db['Name']
            print(f"Processing tables for database: {db_name}")
            
            tables = connector.get_tables(db_name)
            print(f"Found {len(tables)} tables in {db_name}")
            
            table_entries = entry_builder.build_table_entries(config, tables)
            for entry in table_entries:
                file.write(json.dumps(entry) + "\n")
            
            total_tables += len(tables)
        
        print(f"Total entries written: {1 + len(databases) + total_tables}")
    
    # Upload to GCS
    gcs_uploader.upload(config, FILENAME, FOLDERNAME)
    print(f"Successfully uploaded {FILENAME} to GCS")
