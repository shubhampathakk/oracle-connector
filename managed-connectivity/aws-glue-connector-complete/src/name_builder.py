from src.constants import EntryType, SOURCE_TYPE

def create_name(config, entry_type, db_name, asset_name=None):
    """Creates the 'name' for a Dataplex entry within an Entry Group."""
    project = config['project_id']
    location = config['location_id']
    entry_group = config['entry_group_id']
    
    # Sanitize all components used in the name to be valid resource IDs
    db_name_sanitized = db_name.replace('-', '_')
    
    if entry_type == EntryType.DATABASE:
        return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{db_name_sanitized}"
    elif entry_type in [EntryType.TABLE, EntryType.VIEW]:
        asset_name_sanitized = asset_name.replace('.', '_').replace('-', '_')
        return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{db_name_sanitized}_{asset_name_sanitized}"
    raise ValueError(f"Invalid entry_type provided to name_builder: {entry_type}")

def create_fqn(config, entry_type, db_name, asset_name=None):
    """Creates the 'fully_qualified_name' with sanitized path components."""
    system = SOURCE_TYPE
    
    # Sanitize all parts of the path by replacing hyphens with underscores
    region_sanitized = config.get('aws_region', 'aws_region').replace('-', '_')
    db_name_sanitized = db_name.replace('-', '_')

    path_parts = [
        region_sanitized,
        db_name_sanitized
    ]
    if asset_name:
        asset_name_sanitized = asset_name.replace('-', '_')
        path_parts.append(asset_name_sanitized)
    
    path = ".".join(path_parts)
    
    return f"{system}:{path}"

def create_parent_name(config, entry_type, db_name):
    """Creates the 'parent_entry' name."""
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        return create_name(config, EntryType.DATABASE, db_name)
    return None