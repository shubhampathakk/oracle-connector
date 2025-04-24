"""Reads Oracle using PySpark."""
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame

from src.constants import EntryType


# SPARK_JAR_PATH = "/opt/spark/jars/ojdbc11.jar" # Original Path from Dockerfile?
SPARK_JAR_PATH = "./ojdbc11.jar"  # Path for local execution


class OracleConnector:
    """Reads data from Oracle and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint
        self._spark = SparkSession.builder.appName("OracleIngestor") \
            .config("spark.jars", SPARK_JAR_PATH) \
            .getOrCreate()

        self._config = config
        # Use correct JDBC connection string depending on Service vs SID
        if config.get('sid'):  # Check if sid exists and is not empty/None
            self._url = f"jdbc:oracle:thin:@{config['host']}:{config['port']}:{config['sid']}"
        elif config.get('service'): # Check if service exists and is not empty/None
            self._url = f"jdbc:oracle:thin:@{config['host']}:{config['port']}/{config['service']}"
        else:
            # Handle error: neither sid nor service was provided correctly
            raise ValueError("Oracle connection requires either 'sid' or 'service' parameter.")


    def _execute(self, query: str) -> DataFrame:
        """A generic method to execute any query."""
        return self._spark.read.format("jdbc") \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("url", self._url) \
            .option("query", query) \
            .option("user", self._config["user"]) \
            .option("password", self._config["password"]) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        """In Oracle, schemas are usernames."""
        """Query selects all schemas, excluding system schemas"""
        query = """
        SELECT username FROM dba_users WHERE username not in
        ('SYS','SYSTEM','XS$NULL',
        'OJVMSYS','LBACSYS','OUTLN',
        'DBSNMP','APPQOSSYS','DBSFWUSER',
        'GGSYS','ANONYMOUS','CTXSYS',
        'DVSYS','DVF','AUDSYS','GSMADMIN_INTERNAL',
        'OLAPSYS','MDSYS','WMSYS','GSMCATUSER',
        'MDDATA','SYSBACKUP','REMOTE_SCHEDULER_AGENT',
        'GSMUSER','SYSRAC','GSMROOTUSER','SI_INFORMTN_SCHEM',
        'DIP','ORDPLUGINS','SYSKM','SI_INFORMTN_SCHEMA',
        'DGPDB_INT','ORDDATA','ORACLE_OCM',
        'SYS$UMF','SYSD','ORDSYS','SYSDG','PDADMIN')
        """
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Gets a list of columns in tables or views in a batch."""
        # Every line here is a column that belongs to the table or to the view.
        # This SQL gets data from ALL the tables in a given schema.
        return (f"SELECT col.TABLE_NAME, col.COLUMN_NAME, "
                f"col.DATA_TYPE, col.NULLABLE "
                f"FROM all_tab_columns col "
                f"INNER JOIN DBA_OBJECTS tab "
                f"ON tab.OBJECT_NAME = col.TABLE_NAME "
                f"WHERE tab.OWNER = '{schema_name}' "
                f"AND tab.OBJECT_TYPE = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        # Dataset means that these entities can contain end user data.
        short_type = entry_type.name  # table or view, or the title of enum value
        query = self._get_columns(schema_name, short_type)
        return self._execute(query)

    def get_lineage_dependencies(self, schema_names: List[str]) -> DataFrame:
        """Gets object dependencies from ALL_DEPENDENCIES."""
        if not schema_names:
            return self._spark.createDataFrame([], "owner STRING, name STRING, type STRING, referenced_owner STRING, referenced_name STRING, referenced_type STRING")

        # Format schema names for SQL IN clause
        schema_list_str = ", ".join(f"'{name}'" for name in schema_names)

        query = f"""
        SELECT
            owner,          -- Schema of the dependent object
            name,           -- Name of the dependent object
            type,           -- Type of the dependent object (e.g., VIEW, PROCEDURE, FUNCTION)
            referenced_owner, -- Schema of the object being referenced
            referenced_name,  -- Name of the object being referenced
            referenced_type   -- Type of the object being referenced (e.g., TABLE, VIEW)
        FROM
            ALL_DEPENDENCIES
        WHERE
            owner IN ({schema_list_str})
            AND referenced_owner IN ({schema_list_str})
            -- Optional: Add filters for specific types if needed
            -- AND type IN ('VIEW', 'PROCEDURE', 'FUNCTION', 'MATERIALIZED VIEW')
            -- AND referenced_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
            -- Optional: Exclude self-dependencies if necessary
            -- AND (owner != referenced_owner OR name != referenced_name)
        ORDER BY
            owner, name, referenced_owner, referenced_name
        """
        print(f"Executing lineage query for schemas: {schema_list_str}")
        return self._execute(query)