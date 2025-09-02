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
    """Creates the 'fully_qualified_name' with the correct AWS Glue format."""
    system = SOURCE_TYPE # Should be 'aws_glue'
    
    # Get the required values from your config
    aws_account_id = config.get('aws_account_id')
    aws_region = config.get('aws_region')

    if not aws_account_id or not aws_region:
        raise ValueError("AWS Account ID and Region are missing from the configuration.")

    # Sanitize the database name
    db_name_sanitized = db_name.replace('-', '_')

    # For DATABASE type FQN
    if entry_type == EntryType.DATABASE:
        # The FQN for a database is just its hierarchical path.
        path = f"{aws_region}.{aws_account_id}.{db_name_sanitized}"
        return f"{system}:{path}"

    # For TABLE and VIEW type FQNs
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        asset_name_sanitized = asset_name.replace('-', '_')
        # The FQN for a table MUST include the 'table:' prefix.
        path = (f"table:{aws_region}.{aws_account_id}."
                f"{db_name_sanitized}.{asset_name_sanitized}")
        return f"{system}:{path}"

    raise ValueError(f"Invalid entry_type provided to name_builder: {entry_type}")


    # For TABLE and VIEW type FQNs
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        asset_name_sanitized = asset_name.replace('-', '_')
        path = (f"table:{aws_region}.{aws_account_id}."
                f"{db_name_sanitized}.{asset_name_sanitized}")
        return f"{system}:{path}"

    raise ValueError(f"Invalid entry_type provided to name_builder: {entry_type}")

def create_parent_name(config, entry_type, db_name):
    """Creates the 'parent_entry' name."""
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        return create_name(config, EntryType.DATABASE, db_name)
    return None