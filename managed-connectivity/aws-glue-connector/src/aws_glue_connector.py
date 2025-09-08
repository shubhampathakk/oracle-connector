import boto3
import re

class AWSGlueConnector:
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_region):
        self.access_key_id = self._clean_credential(aws_access_key_id)
        self.secret_access_key = self._clean_credential(aws_secret_access_key)
        self.region = aws_region.strip()
        
        try:
            self.__glue_client = boto3.client(
                'glue',
                region_name=self.region,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key
            )
        except Exception as e:
            raise ValueError(f"Failed to create AWS Glue client: {e}")
    
    def _clean_credential(self, credential):
        """Clean and validate credential string"""
        if not credential:
            raise ValueError("Empty credential provided")
        cleaned = re.sub(r'[\r\n\t\s]', '', credential)
        if not cleaned or len(cleaned) < 10:
            raise ValueError("Invalid credential format")
        return cleaned

    def get_databases(self, include_databases=None):
        """Fetches metadata from AWS Glue Data Catalog."""
        if include_databases is None:
            include_databases = []
        metadata = {}
        try:
            paginator = self.__glue_client.get_paginator('get_databases')
            for page in paginator.paginate():
                for db in page['DatabaseList']:
                    db_name = db['Name']
                    if not include_databases or db_name in include_databases:
                        metadata[db_name] = self._get_tables(db_name)
        except Exception as e:
            raise RuntimeError(f"Failed to get databases from AWS Glue: {e}")
        return metadata

    def _get_tables(self, db_name):
        """Fetches tables from a specific database."""
        tables = []
        try:
            paginator = self.__glue_client.get_paginator('get_tables')
            for page in paginator.paginate(DatabaseName=db_name):
                tables.extend(page['TableList'])
        except Exception as e:
            raise RuntimeError(f"Failed to get tables from AWS Glue for database {db_name}: {e}")
        return tables

    def get_lineage_info(self):
        """
        Scans AWS Glue jobs to derive lineage information.
        
        Returns:
            A dictionary mapping target table names to a list of their source table names.
            Example: {'target_table_a': ['source_table_x', 'source_table_y']}
        """
        lineage_map = {}
        paginator = self.glue_client.get_paginator('get_jobs')

        print("Fetching lineage info from AWS Glue jobs...")
        try:
            for page in paginator.paginate():
                for job in page['Jobs']:
                    job_name = job['Name']
                    job_details = self.glue_client.get_job(JobName=job_name)
                    
                    sources = []
                    targets = []

                    # Extract sources and targets from the job's connections/arguments
                    # This logic might need to be adapted based on your specific job setup
                    if 'Connections' in job_details['Job'] and 'Connections' in job_details['Job']['Connections']:
                        # A simplified logic assuming connection names relate to tables
                        # In a real-world scenario, you might parse DefaultArguments
                        pass # Add logic here to find source/target tables

                    # For this example, let's assume we found a simple source/target pair
                    # In your real implementation, you would derive this dynamically
                    # Example:
                    # if job_name == 'job_that_creates_employees_summary':
                    #     sources = ['employees', 'departments']
                    #     targets = ['employees_summary']

                    for target_table in targets:
                        if target_table not in lineage_map:
                            lineage_map[target_table] = []
                        lineage_map[target_table].extend(sources)

        except Exception as e:
            print(f"Warning: Could not fetch lineage information. Error: {e}")
        
        print(f"Found {len(lineage_map)} lineage relationships.")
        return lineage_map