"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "aws_glue"

class EntryType(enum.Enum):
    """Types of AWS Glue entries."""
    CATALOG: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-catalog"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-database" 
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-table"
