import boto3

class AWSGlueConnector:
    """A connector to fetch metadata from AWS Glue Data Catalog."""

    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_region):
        self.__glue_client = boto3.client(
            'glue',
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    def get_metadata(self, include_databases):
        """
        Fetches metadata from AWS Glue Data Catalog.

        :param include_databases: A list of databases to include.
        :return: A dictionary containing the metadata.
        """
        metadata = {}
        paginator = self.__glue_client.get_paginator('get_databases')
        for page in paginator.paginate():
            for db in page['DatabaseList']:
                db_name = db['Name']
                if db_name in include_databases:
                    metadata[db_name] = self._get_tables(db_name)
        return metadata

    def _get_tables(self, database_name):
        """
        Fetches all tables for a given database.

        :param database_name: The name of the database.
        :return: A dictionary of tables.
        """
        tables = {}
        paginator = self.__glue_client.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database_name):
            for table in page['TableList']:
                table_name = table['Name']
                tables[table_name] = {
                    'description': table.get('Description', ''),
                    'columns': self._get_columns(table)
                }
        return tables

    def _get_columns(self, table):
        """
        Extracts column information from a table dictionary.

        :param table: The table dictionary from the Glue API.
        :return: A list of columns.
        """
        columns = []
        if 'StorageDescriptor' in table and 'Columns' in table['StorageDescriptor']:
            for column in table['StorageDescriptor']['Columns']:
                columns.append({
                    'name': column['Name'],
                    'type': column.get('Type', ''),
                    'description': column.get('Comment', '')
                })
        return columns