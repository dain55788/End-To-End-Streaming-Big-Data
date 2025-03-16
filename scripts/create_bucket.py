from minio import Minio
import os
import sys
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from utils.minio_utils import MinIOClient
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Creat MinIO Connection
endpoint_url = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("BUCKET_NAME_1")

client = MinIOClient(
    endpoint_url=endpoint_url,
    access_key=access_key,
    secret_key=secret_key
)

client.create_bucket(bucket_name)

