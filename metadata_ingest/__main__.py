import argparse
from metadata_ingest.ingest import fetch_metadata_and_insert
from lineage_query_extract.db import MSSQLConnection




def main(d):
    # parser = argparse.ArgumentParser(description='Ingest metadata from a database')

    # parser.add_argument('-d', type=str, help='Database')
    # args = parser.parse_args()
    data_engine = MSSQLConnection('localhost', d, 'lin', 'sql')
    fetch_metadata_and_insert(data_engine,d)