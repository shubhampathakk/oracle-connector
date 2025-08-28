import logging

from google.cloud.dataplex import Entry, EntryType, Schema


class EntryBuilder:
    """A builder for Dataplex Entries."""

    def __init__(self, project_id, location_id, lake_id, zone_id):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__lake_id = lake_id
        self.__zone_id = zone_id

    def build_entries(self, metadata):
        """
        Builds Dataplex Entries from AWS Glue metadata.
        """
        entries = []
        for db_name, tables in metadata.items():
            for table_name, table_data in tables.items():
                entry_id = f"{db_name}_{table_name}"
                entry = self._create_entry(entry_id, table_data)
                entries.append(entry)
        return entries

    def _create_entry(self, entry_id, table_data):
        """
        Creates a single Dataplex Entry.
        """
        entry = Entry()
        entry.name = f"projects/{self.__project_id}/locations/{self.__location_id}/lakes/{self.__lake_id}/zones/{self.__zone_id}/entries/{entry_id}"
        entry.entry_type = EntryType.TABLE
        entry.display_name = entry_id
        entry.description = table_data.get('description', '')

        schema = Schema()
        for col_data in table_data.get('columns', []):
            schema.fields.append(
                Schema.SchemaField(
                    name=col_data['name'],
                    type=col_data['type'],
                    mode="NULLABLE",  # Assuming all columns are nullable
                    description=col_data.get('description', '')
                )
            )
        entry.schema_ = schema
        return entry