import os
from dotenv import load_dotenv
from postgre_client import PostgreSQLClient

load_dotenv()

def main():
    pgc = PostgreSQLClient(
        # database=''
    )