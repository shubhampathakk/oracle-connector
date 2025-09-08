from src.aws_glue_connector import AWSGlueConnector
from src.cmd_reader import get_config
import src.entry_builder as eb
from src.gcs_uploader import GCSUploader
import os

def run():
    """Connects to AWS Glue, builds table entries, and uploads the metadata."""
    config = get_config()
    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']
    aws_region = config['aws_region']
    gcs_bucket = config['gcs_bucket']
    project_id = config['project_id']
    output_folder = config.get('output_folder')

    # Connect to AWS Glue
    glue_connector = AWSGlueConnector(aws_access_key_id, aws_secret_access_key, aws_region)
    
    # Fetch metadata
    databases = glue_connector.get_databases()
    job_lineage = glue_connector.get_lineage_info()

    # Build entries - ONLY FOR TABLES AND VIEWS
    all_import_items = []
    for db_name, tables in databases.items():
        for table_info in tables:
            # We now only build dataset (table/view) entries
            table_entry = eb.build_dataset_entry(config, db_name, table_info, job_lineage)
            if table_entry:
                all_import_items.append(table_entry)

    # Upload to GCS
    gcs_uploader = GCSUploader(project_id, gcs_bucket)
    gcs_uploader.upload_entries(
        entries=all_import_items,
        aws_region=aws_region,
        output_folder=output_folder
    )

    final_path = os.path.join(output_folder, f"aws-glue-output-{aws_region}.jsonl") if output_folder else f"aws-glue-output-{aws_region}.jsonl"
    print(f"Successfully uploaded metadata for {len(all_import_items)} tables/views to: gs://{gcs_bucket}/{final_path}")