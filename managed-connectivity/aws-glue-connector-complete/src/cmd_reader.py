"""Command line reader."""
import argparse

def read_args():
    """Reads arguments from the command line."""
    parser = argparse.ArgumentParser()

    # Project arguments
    parser.add_argument("--target_project_id", type=str, required=True,
        help="The name of the target Google Cloud project to import the metadata into.")
    parser.add_argument("--target_location_id", type=str, required=True,
        help="The target Google Cloud location where the metadata will be imported into.")
    parser.add_argument("--target_entry_group_id", type=str, required=True,
        help="The ID of the Dataplex Entry Group to import metadata into.")

    # AWS Glue specific arguments
    parser.add_argument("--aws-region", type=str, required=True,
        help="AWS region where Glue Data Catalog is located")
    parser.add_argument("--aws-access-key-secret", type=str, required=True,
        help="Secret Manager resource name containing AWS access key ID")
    parser.add_argument("--aws-secret-key-secret", type=str, required=True,
        help="Secret Manager resource name containing AWS secret access key")
    parser.add_argument("--include-databases", type=str, required=False,
        help="Comma-separated list of database names to include (optional)")
    parser.add_argument("--exclude-databases", type=str, required=False,
        help="Comma-separated list of database names to exclude (optional)")

    # Google Cloud Storage arguments
    parser.add_argument("--output_bucket", type=str, required=True,
        help="The Cloud Storage bucket to write the generated metadata import file")
    parser.add_argument("--output_folder", type=str, required=True,
        help="The folder within the Cloud Storage bucket")

    return vars(parser.parse_known_args()[0])
