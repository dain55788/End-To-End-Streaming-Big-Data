import os
import sys
import pandas as pd
from minio import Minio
import time
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from utils.minio_utils import MinIOClient
from dotenv import load_dotenv
# Performing data transformation in from MinIO BRONZE --> SILVER

load_dotenv()


def transform_data(endpoint_url, access_key, secret_key):
    """
        Transform data after loading into Datalake (MinIO)
    """
    # Create connection to MinIO
    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    # Create bucket 'sales_silver'
    client.create_bucket(os.getenv('BUCKET_NAME_2'))





