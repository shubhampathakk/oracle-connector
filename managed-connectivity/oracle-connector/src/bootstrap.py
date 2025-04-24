"""The entrypoint of a pipeline."""
from typing import Dict, List
import sys
import json

from datetime import datetime

from src.constants import EntryType
from src.constants import SOURCE_TYPE
from src import cmd_reader
from src import secret_manager
from src import entry_builder # Uses corrected entry_builder
from src import gcs_uploader
from src import top_entry_builder
from src.oracle_connector import OracleConnector # Uses corrected oracle_connector
# name_builder is used implicitly via entry_builder calls

# Define the ID for your custom lineage aspect type
# *** CHANGE THIS if you used a different ID when creating it in Dataplex ***
CUSTOM_LINEAGE_ASPECT_ID = "oracle_lineage"

def write_jsonl_from_list(output_file, json_list):
    """Writes a list of json strings to the file in JSONL format."""
    for json_string in json_list:
        output_file.write(json_string + "\n")


def run():
    """Runs the metadata extraction and lineage pipeline."""
    config = cmd_reader.read_args()

    # Construct the full lineage aspect type resource name (slash-separated)
    # This is mainly stored for potential use if full resource name is needed elsewhere,
    # but the dot-separated version is used for keys/internal types now.
    try:
        full_lineage_aspect_resource_name = (
            f"projects/{config['target_project_id']}/"
            f"locations/{config['target_location_id']}/"
            f"aspectTypes/{CUSTOM_LINEAGE_ASPECT_ID}"
        )
        # Store it in the config for potential use
        config['lineage_aspect_type_resource_name'] = full_lineage_aspect_resource_name

        # Also store the ID itself for easier reference
        config['lineage_aspect_id'] = CUSTOM_LINEAGE_ASPECT_ID

        print(f"Using lineage aspect ID: {CUSTOM_LINEAGE_ASPECT_ID}")
        # The dot-separated name ('project.location.id') will be generated dynamically
        # where needed by name_builder and entry_builder based on the ID and config.

    except KeyError as e:
        print(f"Error: Missing required configuration key: {e}. Cannot construct lineage aspect name.")
        sys.exit(1)


    if not gcs_uploader.checkDestination(config):
        print("Exiting: GCS destination check failed.")
        sys.exit(1) # Use non-zero exit code for errors

    # --- Setup Output ---
    currentDate = datetime.now()
    FOLDERNAME = f"{SOURCE_TYPE}/{currentDate.strftime('%Y%m%d-%H%M%S')}" # Use strftime for consistency
    # Build the output file name from connection details
    db_identifier = config.get('sid') or config.get('service', 'UNKNOWN_DB')
    FILENAME = f"oracle-output-{db_identifier}-{currentDate.strftime('%Y%m%d%H%M%S')}.jsonl" # Add timestamp

    print(f"Output GCS Path: gs://{config['output_bucket']}/{FOLDERNAME}/{FILENAME}")

    # --- Get Credentials ---
    try:
        config["password"] = secret_manager.get_password(config["password_secret"])
    except Exception as ex:
        print(f"Error accessing secret manager: {ex}")
        print("Exiting")
        sys.exit(1)

    # --- Initialize Connector ---
    try:
        connector = OracleConnector(config)
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
        sys.exit(1)
    except Exception as ex:
        print(f"Error initializing Oracle Connector: {ex}")
        sys.exit(1)


    all_entries_json = [] # Store all generated entry JSON strings here

    # --- Generate Top-Level Entries ---
    print("Generating top-level entries (Instance, Database)...")
    try:
        # Ensure top_entry_builder.create returns a valid JSON string or None
        instance_entry = top_entry_builder.create(config, EntryType.INSTANCE)
        if instance_entry: all_entries_json.append(instance_entry)
        database_entry = top_entry_builder.create(config, EntryType.DATABASE)
        if database_entry: all_entries_json.append(database_entry)
    except Exception as e:
         print(f"Error generating top-level entries: {e}")
         # Decide whether to continue or exit

    # --- Process Schemas ---
    print("Fetching schemas...")
    try:
        df_raw_schemas = connector.get_db_schemas()
        if df_raw_schemas is None or df_raw_schemas.rdd.isEmpty(): # More reliable check
            print("No schemas found or accessible with the provided user/privileges.")
            schemas = []
            schemas_json = []
        else:
            df_raw_schemas.persist()
            schemas = [row.USERNAME for row in df_raw_schemas.select("USERNAME").collect()]
            print(f"Found schemas: {schemas}")
            # Pass config to build_schemas
            schemas_df = entry_builder.build_schemas(config, df_raw_schemas)
            if schemas_df: # Check if DF is not None
                 schemas_json = schemas_df.toJSON().collect()
                 all_entries_json.extend(schemas_json)
            else:
                 schemas_json = [] # Ensure it's a list even if empty/None
            df_raw_schemas.unpersist() # Release persisted DF

    except Exception as e:
        print(f"Error fetching or processing schemas: {e}")
        schemas = []
        schemas_json = []

    schemas_count = len(schemas_json)
    print(f"Processed {schemas_count} schemas.")


    # --- Process Tables and Views for each Schema ---
    tables_count = 0
    views_count = 0
    for schema in schemas:
        try:
            print(f"Processing tables for schema: {schema}")
            tables_df = connector.get_dataset(schema, EntryType.TABLE)
            if tables_df is not None and not tables_df.rdd.isEmpty():
                 # Pass config to build_dataset
                 tables_built_df = entry_builder.build_dataset(config, tables_df, schema, EntryType.TABLE)
                 if tables_built_df:
                    tables_json = tables_built_df.toJSON().collect()
                    tables_count += len(tables_json)
                    all_entries_json.extend(tables_json)
            else:
                print(f"No tables found or processed for schema: {schema}")

            print(f"Processing views for schema: {schema}")
            views_df = connector.get_dataset(schema, EntryType.VIEW)
            if views_df is not None and not views_df.rdd.isEmpty():
                 # Pass config to build_dataset
                views_built_df = entry_builder.build_dataset(config, views_df, schema, EntryType.VIEW)
                if views_built_df:
                    views_json = views_built_df.toJSON().collect()
                    views_count += len(views_json)
                    all_entries_json.extend(views_json)
            else:
                print(f"No views found or processed for schema: {schema}")

        except Exception as e:
            print(f"Error processing tables/views for schema {schema}: {e}")
            # Continue to the next schema

    print(f"Processed {tables_count} tables and {views_count} views.")

    # --- Process Lineage ---
    print("Fetching lineage dependencies...")
    # lineage_map will store {target_fqn: python_links_list}
    # where python_links_list is a Python list of Python dictionaries:
    # [{source:{fully_qualified_name:...}, target:{fully_qualified_name:...}}, ...]
    lineage_map = {}
    try:
        if schemas:
            df_raw_dependencies = connector.get_lineage_dependencies(schemas)

            if df_raw_dependencies is not None and not df_raw_dependencies.rdd.isEmpty():
                print("Building lineage link lists...")
                # build_lineage returns target_fqn and links_list (list of Spark Row structs)
                lineage_df = entry_builder.build_lineage(config, df_raw_dependencies)

                if lineage_df:
                    lineage_map_rows = lineage_df.collect()

                    # *** FIX: Explicitly convert collected Spark Rows to Python dicts ***
                    print("Converting collected Spark Rows to Python dictionaries...")
                    temp_lineage_map = {}
                    for row in lineage_map_rows:
                        target_fqn = row.get('target_fqn')
                        spark_links_list = row.get('links_list')
                        if target_fqn and spark_links_list:
                            # Convert each Spark Row in the list to a Python dict
                            python_links_list = [link_row.asDict(recursive=True) for link_row in spark_links_list]
                            temp_lineage_map[target_fqn] = python_links_list
                        else:
                            print(f"Warning: Missing target_fqn or links_list in collected row: {row}")

                    lineage_map = temp_lineage_map # Assign the converted map
                    print(f"Collected and converted lineage links list for {len(lineage_map)} target objects.")
                else:
                    print("Build lineage returned no results.")
            else:
                print("No dependency data returned from Oracle.")
        else:
            print("Skipping lineage query as no schemas were processed.")

    except Exception as e:
        print(f"Error fetching or processing lineage: {e}")


    # --- Merge Lineage and Write Output ---
    final_jsonl_output = []
    merged_lineage_count = 0
    print("Merging lineage information and preparing final output...")

    # Get the dot-separated lineage aspect key name
    lineage_aspect_key_to_use = None
    try:
        lineage_aspect_id = config['lineage_aspect_id']
        lineage_aspect_key_to_use = f"{config['target_project_id']}.{config['target_location_id']}.{lineage_aspect_id}"
    except KeyError:
        print("Warning: Could not determine lineage aspect key name from config. Lineage merging may fail.")


    for entry_json_str in all_entries_json:
        try:
            entry_dict = json.loads(entry_json_str)
            entry_content = entry_dict.get("entry", {})
            target_fqn = entry_content.get("fully_qualified_name")

            # Check if this entry's FQN exists in our lineage map
            if target_fqn and target_fqn in lineage_map and lineage_aspect_key_to_use:
                links_list = lineage_map.get(target_fqn) # Get the list of link dicts

                if links_list: # Check if the list exists and is not empty
                    # *** FIX: Explicitly construct the *entire* aspect object here ***
                    lineage_aspect_object = {
                        "aspect_type": lineage_aspect_key_to_use, # Use dot-separated name
                        "data": {
                            "links": links_list # Assign the collected list directly
                        }
                    }

                    # Ensure the aspects map exists
                    if "aspects" not in entry_content:
                        entry_content["aspects"] = {}

                    # Assign the fully constructed object as the value
                    entry_content["aspects"][lineage_aspect_key_to_use] = lineage_aspect_object

                    # Update aspect_keys list
                    if "aspect_keys" not in entry_dict: entry_dict["aspect_keys"] = []
                    if lineage_aspect_key_to_use not in entry_dict["aspect_keys"]:
                       entry_dict["aspect_keys"].append(lineage_aspect_key_to_use)

                    merged_lineage_count += 1
                    entry_dict["entry"] = entry_content # Put updated content back
                    final_jsonl_output.append(json.dumps(entry_dict))
                else:
                     # No lineage links found for this FQN, add original entry
                     final_jsonl_output.append(entry_json_str)

            else:
                # No lineage for this entry, add as is
                final_jsonl_output.append(entry_json_str)
        except json.JSONDecodeError:
            print(f"Error decoding entry JSON for merging: {entry_json_str[:100]}...")
            final_jsonl_output.append(entry_json_str)
        except Exception as e:
            entry_name_debug = entry_dict.get('entry', {}).get('name', 'UNKNOWN')
            print(f"Unexpected error merging lineage for entry {entry_name_debug}: {e}")
            final_jsonl_output.append(entry_json_str)


    print(f"Merged lineage aspect onto {merged_lineage_count} entries.")
    total_entries = len(final_jsonl_output)
    print(f"Writing {total_entries} total entries to {FILENAME}...")

    try:
        with open(FILENAME, "w", encoding="utf-8") as file:
            write_jsonl_from_list(file, final_jsonl_output)
        print(f"{total_entries} entries written to file {FILENAME}")
    except IOError as e:
        print(f"Error writing output file {FILENAME}: {e}")
        sys.exit(1)


    # --- Upload to GCS ---
    print("Uploading to GCS...")
    try:
        gcs_uploader.upload(config, FILENAME, FOLDERNAME)
        print("Upload complete.")
    except Exception as e:
        print(f"Error uploading {FILENAME} to gs://{config['output_bucket']}/{FOLDERNAME}/ : {e}")
        # Don't exit, file is still available locally

    print("Pipeline finished.")

# Make bootstrap runnable
if __name__ == '__main__':
    run()