"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "aws_glue"
LINEAGE_ASPECT_KEY = "dataplex-types.global.lineage"

class EntryType(enum.Enum):
    """Types of AWS Glue entries."""
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-database"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-view"