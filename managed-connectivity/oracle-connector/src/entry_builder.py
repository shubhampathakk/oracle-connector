"""Creates entries with PySpark."""
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType
import json
from typing import Dict # Import Dict for type hinting

# Assuming these are in the parent directory or configured in sys.path
from src.constants import EntryType, SOURCE_TYPE
from src import name_builder as nb

@F.udf(returnType=StringType())
def choose_metadata_type_udf(data_type: str):
    """Choose the metadata type based on Oracle native type."""
    if data_type is None:
        return "OTHER"
    dt_upper = data_type.upper() # Ensure case-insensitivity
    if dt_upper.startswith("NUMBER") or dt_upper in ["INTEGER","SHORTINTEGER","LONGINTEGER","BINARY_FLOAT","BINARY_DOUBLE","FLOAT", "LONG", "INT", "DECIMAL", "NUMERIC"]:
        return "NUMBER"
    if dt_upper.startswith("VARCHAR") or dt_upper in ["NVARCHAR2","CHAR","NCHAR","CLOB","NCLOB", "STRING"]:
        return "STRING"
    if dt_upper in ["LONG","BLOB","RAW","LONG RAW", "BFILE"]:
        return "BYTES"
    if dt_upper.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if dt_upper == "DATE":
        return "DATETIME"
    return "OTHER"


def create_entry_source(column):
    """Create Entry Source segment."""
    return F.named_struct(F.lit("display_name"),
                          column,
                          F.lit("system"),
                          F.lit(SOURCE_TYPE))


# --- CORRECTED create_entry_aspect (Matches working sample) ---
def create_entry_aspect(config: Dict[str, str], entry_type: EntryType):
    """Create aspect with general information (usually it is empty).
       Uses dot-separated name for BOTH the map key and the internal aspect_type field."""

    # Generate the dot-separated name (e.g., project.location.id)
    # Uses the corrected name_builder function provided previously
    aspect_name_key = nb.create_entry_aspect_name(config, entry_type) # Returns project.location.id

    # Construct the aspect structure using the dot-separated name for aspect_type
    aspect_value_struct = F.named_struct(
            F.lit("aspect_type"), F.lit(aspect_name_key), # Use dot-separated name here too
            F.lit("data"), F.create_map() # Empty data map for simple aspects
    )

    # Create the map with the dot-separated key and the structured value
    return F.create_map( F.lit(aspect_name_key), aspect_value_struct )


def convert_to_import_items(df, aspect_keys):
    """Convert entries to import items."""
    entry_columns = ["name", "fully_qualified_name", "parent_entry",
                     "entry_source", "aspects", "entry_type"]

    # Puts entry to "entry" key, a list of keys from aspects in "aspects_keys"
    # and "aspects" string in "update_mask"
    return df.withColumn("entry", F.struct(entry_columns)) \
      .withColumn("aspect_keys", F.array([F.lit(key) for key in aspect_keys])) \
      .withColumn("update_mask", F.array(F.lit("aspects"))) \
      .drop(*entry_columns)


def build_schemas(config, df_raw_schemas):
    """Create a dataframe with database schemas from the list of usernames."""
    entry_type = EntryType.DB_SCHEMA
    # Generate dot-separated key for the entry-specific aspect
    entry_aspect_name_key = nb.create_entry_aspect_name(config, entry_type)

    parent_name =  nb.create_parent_name(config, entry_type)

    create_name_udf = F.udf(lambda x: nb.create_name(config, entry_type, x), StringType())
    create_fqn_udf = F.udf(lambda x: nb.create_fqn(config, entry_type, x), StringType())

    full_entry_type = entry_type.value.format(
        project=config["target_project_id"],
        location=config["target_location_id"])

    column = F.col("USERNAME")
    df = df_raw_schemas.withColumn("name", create_name_udf(column)) \
      .withColumn("fully_qualified_name", create_fqn_udf(column)) \
      .withColumn("parent_entry", F.lit(parent_name)) \
      .withColumn("entry_type", F.lit(full_entry_type)) \
      .withColumn("entry_source", create_entry_source(column)) \
      .withColumn("aspects", create_entry_aspect(config, entry_type)) \
      .drop(column) # Use corrected create_entry_aspect

    # Use the dot-separated key in the aspect_keys list
    df = convert_to_import_items(df, [entry_aspect_name_key])
    return df


def build_dataset(config, df_raw, db_schema, entry_type):
    """Build table/view entries from a flat list of columns."""

    # --- CORRECTED Schema Key Handling ---
    # **** SET THIS BASED ON YOUR ACTUAL SETUP ****
    use_global_schema = True # Set to False if you created a custom 'schema' aspect

    global_schema_key = "dataplex-types.global.schema"
    schema_aspect_type_name = global_schema_key # Default to global

    if not use_global_schema:
        custom_schema_aspect_id = "schema" # Assuming custom schema ID is 'schema'
        # Generate dot-separated key/name for custom schema aspect
        schema_key = f"{config['target_project_id']}.{config['target_location_id']}.{custom_schema_aspect_id}"
        schema_aspect_type_name = schema_key # Use dot-separated for custom type as well
    else:
        schema_key = global_schema_key
        # For global type, the internal aspect_type name is also the key
        schema_aspect_type_name = global_schema_key

    # Check if df_raw is empty or None
    if df_raw is None or df_raw.rdd.isEmpty():
        print(f"No columns found for schema '{db_schema}', type '{entry_type.name}'. Skipping.")
        return None

    # --- Column Processing ---
    df = df_raw \
      .withColumn("mode", F.when(F.col("NULLABLE") == 'Y', "NULLABLE").otherwise("REQUIRED")) \
      .drop("NULLABLE") \
      .withColumnRenamed("DATA_TYPE", "dataType") \
      .withColumn("metadataType", choose_metadata_type_udf(F.col("dataType"))) \
      .withColumnRenamed("COLUMN_NAME", "name")

    aspect_columns = ["name", "mode", "dataType", "metadataType"]
    df = df.withColumn("columns", F.struct(aspect_columns))\
      .groupby('TABLE_NAME') \
      .agg(F.collect_list("columns").alias("fields"))

    # --- CORRECTED Aspect Map Creation ---
    # Create the schema aspect map (using correct key and internal aspect_type name)
    schema_aspect_data = F.create_map(F.lit("fields"), F.col("fields"))
    schema_aspect_value_struct = F.named_struct(
        F.lit("aspect_type"), F.lit(schema_aspect_type_name), # Use correct name
        F.lit("data"), schema_aspect_data
    )
    schema_map = F.create_map(F.lit(schema_key), schema_aspect_value_struct) # Use correct key

    # Create the entry type specific aspect map using the corrected function
    # This correctly uses dot-separated format for key and internal aspect_type
    entry_aspect_name_key = nb.create_entry_aspect_name(config, entry_type) # Get dot-sep key
    entry_aspect_map = create_entry_aspect(config, entry_type) # Use corrected function

    # Combine aspects
    df = df.withColumn("aspects", F.map_concat(schema_map, entry_aspect_map)) \
           .drop("fields")

    # --- Entry Info Generation ---
    create_name_udf = F.udf(lambda table_name: nb.create_name(config, entry_type, db_schema, table_name), StringType())
    create_fqn_udf = F.udf(lambda table_name: nb.create_fqn(config, entry_type, db_schema, table_name), StringType())
    parent_name = nb.create_parent_name(config, entry_type, db_schema)
    full_entry_type = entry_type.value.format(project=config["target_project_id"], location=config["target_location_id"])
    column = F.col("TABLE_NAME")
    df = df.withColumn("name", create_name_udf(column)) \
      .withColumn("fully_qualified_name", create_fqn_udf(column)) \
      .withColumn("entry_type", F.lit(full_entry_type)) \
      .withColumn("parent_entry", F.lit(parent_name)) \
      .withColumn("entry_source", create_entry_source(column)) \
      .drop(column)

    # --- CORRECTED Final Aspect Keys ---
    # List of keys used in the final 'aspects' map
    final_aspect_keys = [
        schema_key, # Key used for schema aspect
        entry_aspect_name_key # Dot-separated key for entry-specific aspect
    ]

    df_final = convert_to_import_items(df, final_aspect_keys) # Pass the correct list of keys
    return df_final


# --- CORRECTED build_lineage function (Returning ONLY links list) ---
def build_lineage(config, df_raw_dependencies):
    """Builds lineage aspects from a dependency DataFrame.
       Returns only the target FQN and the raw list of link objects."""
    if df_raw_dependencies is None or df_raw_dependencies.rdd.isEmpty():
        print("No dependencies found to build lineage.")
        return None

    # --- Define UDFs for generating FQNs ---
    def get_entry_type_from_oracle(oracle_type: str) -> EntryType:
        type_upper = oracle_type.upper() if oracle_type else ""
        if type_upper == "TABLE": return EntryType.TABLE
        elif type_upper == "VIEW": return EntryType.VIEW
        elif type_upper == "MATERIALIZED VIEW": return EntryType.VIEW
        else: return None

    @F.udf(returnType=StringType())
    def generate_fqn_udf(owner, name, type_str):
        if owner is None or name is None or type_str is None: return None
        entry_type_enum = get_entry_type_from_oracle(type_str)
        if entry_type_enum:
            try: return nb.create_fqn(config, entry_type_enum, owner, name)
            except Exception: return None
        return None

    # --- Apply UDFs ---
    df_with_fqns = df_raw_dependencies \
        .withColumn("source_fqn", generate_fqn_udf(F.col("referenced_owner"), F.col("referenced_name"), F.col("referenced_type"))) \
        .withColumn("target_fqn", generate_fqn_udf(F.col("owner"), F.col("name"), F.col("type")))
    df_filtered = df_with_fqns.filter(F.col("source_fqn").isNotNull() & F.col("target_fqn").isNotNull())

    # --- Create link structure ---
    source_struct = F.struct(F.col("source_fqn").alias("fully_qualified_name")).alias("source")
    target_struct = F.struct(F.col("target_fqn").alias("fully_qualified_name")).alias("target")
    link_struct = F.struct(source_struct, target_struct).alias("link") # This creates the {source:{...}, target:{...}} struct
    df_with_links = df_filtered.withColumn("link_data", link_struct)

    # --- Group by target FQN and collect the raw link structs ---
    df_final = df_with_links.groupBy("target_fqn") \
        .agg(F.collect_list("link_data").alias("links_list")) \
        .select("target_fqn", "links_list") # Return FQN and the list of link structs

    return df_final