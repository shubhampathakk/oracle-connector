"""Builds Dataplex hierarchy identifiers for AWS Glue."""
from typing import Dict
from src.constants import EntryType, SOURCE_TYPE

def create_fqn(config: Dict[str, str], entry_type: EntryType,
               db_name: str = "", table_name: str = ""):
    """Creates a fully qualified name for AWS Glue entities."""
    
    if entry_type == EntryType.CATALOG:
        return f"{SOURCE_TYPE}:`{config['aws_region']}`"
    elif entry_type == EntryType.DATABASE:
        catalog = create_fqn(config, EntryType.CATALOG)
        return f"{catalog}.{db_name}"
    elif entry_type == EntryType.TABLE:
        catalog = create_fqn(config, EntryType.CATALOG)
        return f"{catalog}.{db_name}.{table_name}"
    
    return ""

def create_name(config: Dict[str, str], entry_type: EntryType,
                db_name: str = "", table_name: str = ""):
    """Creates a Dataplex v2 hierarchy name for AWS Glue entities."""
    
    name_prefix = (
        f"projects/{config['target_project_id']}/"
        f"locations/{config['target_location_id']}/"
        f"entryGroups/{config['target_entry_group_id']}/"
        f"entries/"
    )
    
    if entry_type == EntryType.CATALOG:
        return name_prefix + f"aws-glue-catalog-{config['aws_region']}"
    elif entry_type == EntryType.DATABASE:
        catalog = create_name(config, EntryType.CATALOG)
        return f"{catalog}/databases/{db_name}"
    elif entry_type == EntryType.TABLE:
        database = create_name(config, EntryType.DATABASE, db_name)
        return f"{database}/tables/{table_name}"
    
    return ""

def create_parent_name(config: Dict[str, str], entry_type: EntryType,
                       parent_name: str = ""):
    """Generates a Dataplex v2 name of the parent."""
    if entry_type == EntryType.DATABASE:
        return create_name(config, EntryType.CATALOG)
    elif entry_type == EntryType.TABLE:
        return create_name(config, EntryType.DATABASE, parent_name)
    
    return ""

def create_entry_aspect_name(config: Dict[str, str], entry_type: EntryType):
    """Generates an entry aspect name."""
    last_segment = entry_type.value.split("/")[-1]
    return f"{config['target_project_id']}.{config['target_location_id']}.{last_segment}"
