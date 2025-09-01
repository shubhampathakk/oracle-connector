from src.constants import EntryType

def create_name(config, entry_type, db_name, asset_name=None):
    """Creates the 'name' for a Dataplex entry within an Entry Group."""
    project = config['project_id']
    location = config['location_id']
    entry_group = config['entry_group_id']
    
    if entry_type == EntryType.DATABASE:
        return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{db_name}"
    elif entry_type in [EntryType.TABLE, EntryType.VIEW]:
        # Sanitize asset_name to be a valid resource ID segment
        sanitized_asset_name = asset_name.replace('.', '_')
        return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{db_name}_{sanitized_asset_name}"
    raise ValueError(f"Invalid entry_type provided to name_builder: {entry_type}")

def create_fqn(config, entry_type, db_name, asset_name=None):
    """Creates the 'fully_qualified_name' for a Dataplex entry."""
    if asset_name:
        return f"{config.get('aws_account_id', 'aws')}.{config.get('aws_region', 'region')}.{db_name}.{asset_name}"
    return f"{config.get('aws_account_id', 'aws')}.{config.get('aws_region', 'region')}.{db_name}"

def create_parent_name(config, entry_type, db_name):
    """Creates the 'parent_entry' name."""
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        return create_name(config, EntryType.DATABASE, db_name)
    return None