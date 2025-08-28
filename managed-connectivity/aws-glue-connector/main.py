import argparse
import json
import logging
import sys

from src import bootstrap, cmd_reader, gcs_uploader
from src.aws_glue_connector import AWSGlueConnector

# Allow shared files to be found when running from command line
sys.path.insert(1, '../src/shared')

if __name__ == '__main__':
    bootstrap.run()
