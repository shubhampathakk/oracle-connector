import argparse
import json
from typing import Dict

def get_config() -> Dict[str, str]:
    """Reads configuration from a JSON file specified by the --config argument."""
    parser = argparse.ArgumentParser(description="Read config for AWS Glue connector.")
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the JSON config file."
    )
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        return json.load(f)