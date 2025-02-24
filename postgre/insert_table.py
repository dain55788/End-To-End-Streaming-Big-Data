import os
from dotenv import load_dotenv
from sqlserver_to_pg import PostgreSQLClient

load_dotenv()

def main():
    pgc = PostgreSQLClient(
        # database=''
    )