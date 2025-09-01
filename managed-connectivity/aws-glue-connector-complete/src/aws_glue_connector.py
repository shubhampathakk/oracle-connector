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
        """Fetches lineage information from AWS Glue jobs."""
        lineage = {}
        try:
            paginator = self.__glue_client.get_paginator('get_jobs')
            for page in paginator.paginate():
                for job in page['Jobs']:
                    job_name = job['Name']
                    if 'Command' in job and 'ScriptLocation' in job['Command']:
                        # This is where you would add logic to parse the ETL script
                        # For now, we'll focus on jobs with defined sources and sinks
                        pass

                    if 'CodeGenConfigurationNodes' in job:
                        nodes = job['CodeGenConfigurationNodes']
                        sources = []
                        sinks = []
                        
                        for node in nodes.values():
                            if node['NodeType'] == 'DataSource':
                                sources.append(node['DataSource']['Name'])
                            elif node['NodeType'] == 'DataSink':
                                sinks.append(node['DataSink']['Name'])
                        
                        if sources and sinks:
                            for sink in sinks:
                                if sink not in lineage:
                                    lineage[sink] = []
                                lineage[sink].extend(sources)
        except Exception as e:
            raise RuntimeError(f"Failed to get lineage info from AWS Glue jobs: {e}")
        return lineage