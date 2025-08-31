"""AWS Glue Data Catalog connector using boto3."""
import boto3
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class AWSGlueConnector:
    """Reads metadata from AWS Glue Data Catalog."""

    def __init__(self, config: Dict[str, str]):
        self._config = config
        self._glue_client = boto3.client(
            'glue',
            region_name=config['aws_region'],
            aws_access_key_id=config['aws_access_key'],
            aws_secret_access_key=config['aws_secret_key']
        )

    def get_databases(self) -> List[Dict]:
        """Get all databases from Glue Data Catalog."""
        databases = []
        paginator = self._glue_client.get_paginator('get_databases')
        
        for page in paginator.paginate():
            for db in page['DatabaseList']:
                db_name = db['Name']
                
                # Apply include/exclude filters
                if self._should_include_database(db_name):
                    databases.append({
                        'Name': db_name,
                        'Description': db.get('Description', ''),
                        'LocationUri': db.get('LocationUri', ''),
                        'Parameters': db.get('Parameters', {})
                    })
        
        return databases

    def get_tables(self, database_name: str) -> List[Dict]:
        """Get all tables in a database."""
        tables = []
        paginator = self._glue_client.get_paginator('get_tables')
        
        for page in paginator.paginate(DatabaseName=database_name):
            for table in page['TableList']:
                table_data = {
                    'Name': table['Name'],
                    'Description': table.get('Description', ''),
                    'DatabaseName': database_name,
                    'StorageDescriptor': table.get('StorageDescriptor', {}),
                    'Parameters': table.get('Parameters', {}),
                    'TableType': table.get('TableType', 'EXTERNAL_TABLE')
                }
                tables.append(table_data)
        
        return tables

    def _should_include_database(self, db_name: str) -> bool:
        """Check if database should be included based on include/exclude filters."""
        include_databases = self._config.get('include_databases')
        exclude_databases = self._config.get('exclude_databases')
        
        if include_databases:
            include_list = [db.strip() for db in include_databases.split(',')]
            if db_name not in include_list:
                return False
                
        if exclude_databases:
            exclude_list = [db.strip() for db in exclude_databases.split(',')]
            if db_name in exclude_list:
                return False
                
        return True
