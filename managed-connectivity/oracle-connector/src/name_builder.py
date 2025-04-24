"""Builds Dataplex hierarchy identifiers."""
from typing import Dict
# Ensure these imports are correct based on your project structure
from src.constants import EntryType, SOURCE_TYPE


# Oracle cluster users start with C## prefix, but Dataplex doesn't accept #.
# In that case in names it is changed to C!!, and escaped with backticks in FQNs
FORBIDDEN_SYMBOL = "#"
ALLOWED_SYMBOL = "!"


# --- CORRECTED get_database function ---
def get_database(config: Dict[str, str]) -> str:
    """Correctly returns the database identifier (service or SID) provided in the config."""
    service_name = config.get("service")
    sid = config.get("sid")

    if service_name:
        return service_name
    elif sid:
        return sid
    else:
        # This case should ideally not happen due to argparse mutual exclusion,
        # but adding a fallback prevents returning None.
        raise ValueError("Configuration error: Neither '--service' nor '--sid' was provided correctly.")


# --- CORRECTED create_fqn function (using corrected get_database) ---
def create_fqn(config: Dict[str, str], entry_type: EntryType,
               schema_name: str = "", table_name: str = ""):
    """Creates a fully qualified name or Dataplex v1 hierarchy name."""
    db_name = get_database(config) # Get correct DB name first
    if FORBIDDEN_SYMBOL in schema_name:
        schema_name = f"`{schema_name}`" # Escape schema name if needed

    if entry_type == EntryType.INSTANCE:
        host_identifier = config.get("host", "unknown_host")
        # Ensure host identifier is escaped if it contains special chars like ':'
        # Although ':' might be allowed here in FQN depending on source type handling
        return f"{SOURCE_TYPE}:`{host_identifier}`" # Keep escaping for safety
    if entry_type == EntryType.DATABASE:
        instance = create_fqn(config, EntryType.INSTANCE)
        return f"{instance}.{db_name}" # Use corrected db_name
    if entry_type == EntryType.DB_SCHEMA:
        # Based on previous samples/errors, FQN format seems to be db.schema
        return f"{db_name}.{schema_name}" # Use corrected db_name
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        # Based on previous samples/errors, FQN format seems to be db.schema.table
        return f"{db_name}.{schema_name}.{table_name}" # Use corrected db_name
    # Return empty string or raise error for unhandled types
    print(f"Warning: Unhandled EntryType '{entry_type}' in create_fqn")
    return ""


# --- CORRECTED create_name function (using corrected get_database) ---
def create_name(config: Dict[str, str], entry_type: EntryType,
                schema_name: str = "", table_name: str = ""):
    """Creates a Dataplex v2 hierarchy name (full resource path)."""
    db_name = get_database(config) # Get correct DB name first
    if FORBIDDEN_SYMBOL in schema_name:
        schema_name = schema_name.replace(FORBIDDEN_SYMBOL, ALLOWED_SYMBOL)

    # Base path for entries within the entry group
    try:
        entry_group_prefix = (
            f"projects/{config['target_project_id']}/"
            f"locations/{config['target_location_id']}/"
            f"entryGroups/{config['target_entry_group_id']}/entries"
        )
    except KeyError as e:
        raise ValueError(f"Missing required configuration key in config dict: {e}") from e


    # Host part used for instance and database path construction
    # Replace ':' which is invalid in Dataplex resource name segments
    host_part = config.get("host", "unknown_host").replace(":", "@")

    if entry_type == EntryType.INSTANCE:
        return f"{entry_group_prefix}/{host_part}"
    if entry_type == EntryType.DATABASE:
        instance_name = create_name(config, EntryType.INSTANCE) # Get full instance path
        return f"{instance_name}/databases/{db_name}" # Use corrected db_name
    if entry_type == EntryType.DB_SCHEMA:
        database_name = create_name(config, EntryType.DATABASE) # Get full database path
        return f"{database_name}/database_schemas/{schema_name}"
    if entry_type == EntryType.TABLE:
        db_schema_name = create_name(config, EntryType.DB_SCHEMA, schema_name) # Get full schema path
        return f"{db_schema_name}/tables/{table_name}"
    if entry_type == EntryType.VIEW:
        db_schema_name = create_name(config, EntryType.DB_SCHEMA, schema_name) # Get full schema path
        return f"{db_schema_name}/views/{table_name}"
    # Return empty string or raise error for unhandled types
    print(f"Warning: Unhandled EntryType '{entry_type}' in create_name")
    return ""


# --- CORRECTED create_parent_name function (using corrected create_name) ---
def create_parent_name(config: Dict[str, str], entry_type: EntryType,
                       parent_schema_name: str = ""): # Renamed arg for clarity
    """Generates a Dataplex v2 name (full resource path) of the parent entry."""
    if entry_type == EntryType.DATABASE:
        # Parent is INSTANCE
        return create_name(config, EntryType.INSTANCE)
    if entry_type == EntryType.DB_SCHEMA:
        # Parent is DATABASE
        return create_name(config, EntryType.DATABASE)
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        # Parent is DB_SCHEMA
        # Use the provided parent_schema_name here
        return create_name(config, EntryType.DB_SCHEMA, parent_schema_name)
    # Instance entry has no parent_entry in the spec
    return ""


# --- REVERTED create_entry_aspect_name function (using your original logic) ---
def create_entry_aspect_name(config: Dict[str, str], entry_type: EntryType):
    """Generates an entry aspect name in the format 'project.location.aspectId'."""
    # This logic comes from your original code and matches the working sample JSONL.
    try:
        # Extract the short ID (e.g., "oracle-instance") from the Enum value's path
        aspect_id = entry_type.value.split("/")[-1]
        # Construct the dot-separated format
        dot_separated_aspect_name = (
            f"{config['target_project_id']}."
            f"{config['target_location_id']}."
            f"{aspect_id}"
        )
        return dot_separated_aspect_name
    except KeyError as e:
         raise ValueError(f"Missing required configuration key in config dict for create_entry_aspect_name: {e}") from e
    except AttributeError:
         # Handle cases where entry_type might not be a valid Enum with a 'value'
         raise TypeError(f"Invalid entry_type provided to create_entry_aspect_name: {entry_type}") from None