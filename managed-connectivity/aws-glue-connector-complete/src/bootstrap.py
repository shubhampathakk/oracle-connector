from src.aws_glue_connector import AWSGlueConnector
from src.cmd_reader import get_config
import src.entry_builder as eb
from src.gcs_uploader import GCSUploader
import os

def run():
    """Connects to AWS Glue, builds and uploads the metadata."""
    config = get_config()
    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']
    aws_region = config['aws_region']
    gcs_bucket = config['gcs_bucket']
    project_id = config['project_id']
    
    # --- CHANGE 1: Get the output folder from the config ---
    output_folder = config.get('output_folder')

    # Connect to AWS Glue
    glue_connector = AWSGlueConnector(aws_access_key_id, aws_secret_access_key, aws_region)
    
    # Fetch metadata and lineage
    databases = glue_connector.get_databases()
    job_lineage = glue_connector.get_lineage_info()

    # Build entries
    all_import_items = []
    for db_name, tables in databases.items():
        all_import_items.append(eb.build_database_entry(config, db_name))
        
        for table_info in tables:
            all_import_items.append(eb.build_dataset_entry(config, db_name, table_info, job_lineage))

    # Upload to GCS
    gcs_uploader = GCSUploader(project_id, gcs_bucket)
    
    # --- CHANGE 2: Pass the new variables to the uploader ---
    gcs_uploader.upload_entries(
        entries=all_import_items,
        aws_region=aws_region,
        output_folder=output_folder
    )

    # Construct the full path for the log message
    final_path = os.path.join(output_folder, f"aws-glue-output-{aws_region}.jsonl") if output_folder else f"aws-glue-output-{aws_region}.jsonl"
    print(f"Successfully uploaded metadata for {len(databases)} databases to: gs://{gcs_bucket}/{final_path}")