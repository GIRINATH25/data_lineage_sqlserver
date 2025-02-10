import re

def extract_column_lineage(query):
    """
    Extract column lineage from a given SQL query.
    This basic implementation captures SELECT columns and their relationships.
    """
    select_pattern = r"SELECT\s+(.+?)\s+FROM"
    matches = re.search(select_pattern, query, re.IGNORECASE)
    columns = []

    if matches:
        column_list = matches.group(1).split(",")
        columns = [col.strip() for col in column_list]

    return columns
