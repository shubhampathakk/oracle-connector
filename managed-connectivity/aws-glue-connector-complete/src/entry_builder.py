"""Creates entries for AWS Glue metadata."""
import json
from typing import Dict, List
from src.constants import EntryType, SOURCE_TYPE
from src import name_builder as nb

def build_catalog_entry(config: Dict[str, str]) -> Dict:
    """Build AWS Glue catalog entry."""
    entry_type = EntryType.CATALOG
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)
    
    entry = {
        "name": nb.create_name(config, entry_type),
        "entry_type": entry_type.value.format(
            project=config["target_project_id"],
            location=config["target_location_id"]
        ),
        "fully_qualified_name": nb.create_fqn(config, entry_type),
        "parent_entry": "",
        "entry_source": {
            "display_name": "AWS Glue Data Catalog",
            "system": SOURCE_TYPE
        },
        "aspects": {
            entry_aspect_name: {
                "aspect_type": entry_aspect_name,
                "data": {}
            }
        }
    }
    
    return {
        "entry": entry,
        "aspect_keys": [entry_aspect_name],
        "update_mask": ["aspects"]
    }

def build_database_entries(config: Dict[str, str], databases: List[Dict]) -> List[Dict]:
    """Build database entries from AWS Glue databases."""
    entries = []
    entry_type = EntryType.DATABASE
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)
    
    for db in databases:
        db_name = db['Name']
        entry = {
            "name": nb.create_name(config, entry_type, db_name),
            "entry_type": entry_type.value.format(
                project=config["target_project_id"],
                location=config["target_location_id"]
            ),
            "fully_qualified_name": nb.create_fqn(config, entry_type, db_name),
            "parent_entry": nb.create_parent_name(config, entry_type),
            "entry_source": {
                "display_name": db_name,
                "system": SOURCE_TYPE
            },
            "aspects": {
                entry_aspect_name: {
                    "aspect_type": entry_aspect_name,
                    "data": {
                        "description": db.get('Description', ''),
                        "location_uri": db.get('LocationUri', ''),
                        "parameters": db.get('Parameters', {})
                    }
                }
            }
        }
        
        entries.append({
            "entry": entry,
            "aspect_keys": [entry_aspect_name],
            "update_mask": ["aspects"]
        })
    
    return entries

def build_table_entries(config: Dict[str, str], tables: List[Dict]) -> List[Dict]:
    """Build table entries from AWS Glue tables."""
    entries = []
    entry_type = EntryType.TABLE
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)
    schema_aspect_name = "dataplex-types.global.schema"
    
    for table in tables:
        table_name = table['Name']
        db_name = table['DatabaseName']
        
        # Build schema fields
        fields = []
        storage_descriptor = table.get('StorageDescriptor', {})
        columns = storage_descriptor.get('Columns', [])
        
        for column in columns:
            field = {
                "name": column['Name'],
                "dataType": column.get('Type', 'string'),
                "metadataType": map_glue_type_to_metadata_type(column.get('Type', 'string')),
                "mode": "NULLABLE",
                "description": column.get('Comment', '')
            }
            fields.append(field)
        
        entry = {
            "name": nb.create_name(config, entry_type, db_name, table_name),
            "entry_type": entry_type.value.format(
                project=config["target_project_id"],
                location=config["target_location_id"]
            ),
            "fully_qualified_name": nb.create_fqn(config, entry_type, db_name, table_name),
            "parent_entry": nb.create_parent_name(config, entry_type, db_name),
            "entry_source": {
                "display_name": table_name,
                "system": SOURCE_TYPE
            },
            "aspects": {
                schema_aspect_name: {
                    "aspect_type": schema_aspect_name,
                    "data": {
                        "fields": fields
                    }
                },
                entry_aspect_name: {
                    "aspect_type": entry_aspect_name,
                    "data": {
                        "description": table.get('Description', ''),
                        "table_type": table.get('TableType', 'EXTERNAL_TABLE'),
                        "location": storage_descriptor.get('Location', ''),
                        "input_format": storage_descriptor.get('InputFormat', ''),
                        "output_format": storage_descriptor.get('OutputFormat', ''),
                        "parameters": table.get('Parameters', {})
                    }
                }
            }
        }
        
        entries.append({
            "entry": entry,
            "aspect_keys": [schema_aspect_name, entry_aspect_name],
            "update_mask": ["aspects"]
        })
    
    return entries

def map_glue_type_to_metadata_type(glue_type: str) -> str:
    """Map AWS Glue data types to Dataplex metadata types."""
    glue_type_lower = glue_type.lower()
    
    if glue_type_lower in ['bigint', 'int', 'smallint', 'tinyint', 'double', 'float', 'decimal']:
        return "NUMBER"
    elif glue_type_lower in ['string', 'char', 'varchar']:
        return "STRING"
    elif glue_type_lower in ['boolean']:
        return "BOOLEAN"
    elif glue_type_lower in ['date']:
        return "DATE"
    elif glue_type_lower in ['timestamp']:
        return "TIMESTAMP"
    elif glue_type_lower in ['binary']:
        return "BYTES"
    else:
        return "OTHER"
