import sqlparse
import re
from lineage_ingest.lineage import populate_table_relation

def extract_insert_tables(query):
    tables = []
    destination_table = re.search(r'INSERT INTO\s+(\w+)', query, re.IGNORECASE)
    if destination_table:
        destination_table = destination_table.group(1)

    alias_mapping = {}
    table_alias_matches = re.findall(r'FROM\s+(\w+)\s+(\w+)', query, re.IGNORECASE)
    for table_name, alias in table_alias_matches:
        alias_mapping[alias] = table_name

    source_tables = re.findall(r'JOIN\s+(\w+)|FROM\s+(\w+)', query, re.IGNORECASE)
    source_tables = [alias_mapping.get(table, table) for group in source_tables for table in group if table]

    if destination_table:
        destination_table = alias_mapping.get(destination_table, destination_table)
        return {destination_table: source_tables}
    return {}


def extract_update_tables(query):
    tables = []
    destination_table_match = re.search(r'UPDATE\s+(\w+)', query, re.IGNORECASE)
    destination_table = destination_table_match.group(1) if destination_table_match else None

    # Replace alias with the actual table name
    alias_mapping = {}
    table_alias_matches = re.findall(r'FROM\s+(\w+)\s+(\w+)', query, re.IGNORECASE)
    for table_name, alias in table_alias_matches:
        alias_mapping[alias] = table_name

    source_tables = re.findall(r'JOIN\s+(\w+)|FROM\s+(\w+)', query, re.IGNORECASE)
    source_tables = [alias_mapping.get(table, table) for group in source_tables for table in group if table]

    if destination_table in alias_mapping:
        destination_table = alias_mapping[destination_table]

    if destination_table in source_tables:
        source_tables.remove(destination_table)

    if destination_table:
        return {destination_table: source_tables}
    return {}


def extract_merge_tables(query):
    destination_table = re.search(r'MERGE INTO\s+(\w+)', query, re.IGNORECASE)
    if destination_table:
        destination_table = destination_table.group(1)

    alias_mapping = {}
    table_alias_matches = re.findall(r'FROM\s+(\w+)\s+(\w+)', query, re.IGNORECASE)
    for table_name, alias in table_alias_matches:
        alias_mapping[alias] = table_name

    # Extract nested SELECT tables within USING clause
    source_tables = []
    nested_sources = re.findall(r'\(SELECT.*?FROM\s+(\w+)(?:\s+(\w+))?', query, re.IGNORECASE)
    for table, alias in nested_sources:
        source_tables.append(alias_mapping.get(alias, table))

    join_sources = re.findall(r'JOIN\s+(\w+)', query, re.IGNORECASE)
    source_tables.extend(alias_mapping.get(table, table) for table in join_sources)

    source_tables = list(set(source_tables))  # Remove duplicates

    if destination_table:
        destination_table = alias_mapping.get(destination_table, destination_table)
        return {destination_table: source_tables}
    return {}


def extract(query):
    if query.lower().strip().startswith('insert'):
        return extract_insert_tables(query)
    elif query.lower().strip().startswith('update'):
        return extract_update_tables(query)
    elif query.lower().strip().startswith('merge'):
        return extract_merge_tables(query)
    return {}


# query = '''SELECT * FROM table1 WHERE column1 = 1; 

#             INSERT INTO TAB SELECT * FROM table2 JOIN table3 ON table2.id = table3.id; 

#             UPDATE T1 SET T1.COL1 = T2.COL2, T1.COL4 = T3.COL4 FROM table1 T1 JOIN table2 T2 ON T1.COL3 = T2.COL3 JOIN table3 T3 ON T1.COL5 = T3.COL5 WHERE T1.COL6 = 'some_value';

#             MERGE INTO table1 T1 USING (SELECT T2.COL2, T3.COL4, T1.COL3 FROM table2 T2 JOIN table3 T3 ON T2.COL3 = T3.COL3) AS Source ON (T1.COL3 = Source.COL3) WHEN MATCHED THEN UPDATE SET T1.COL1 = Source.COL2, T1.COL4 = Source.COL4;

#             INSERT INTO FinalTable (col1, col2)
# SELECT A.col1, B.col2 
# FROM (SELECT * FROM tableA) A
# JOIN (SELECT col2 FROM tableB) B ON A.id = B.id;

# UPDATE T1 
# SET T1.col1 = X.col1
# FROM table1 T1 
# JOIN (SELECT table2.col1, table3.col2 FROM table2 JOIN table3 ON table2.ref = table3.ref) X 
# ON T1.key = X.key
# WHERE T1.status = 'active';

# MERGE INTO target_table TT
# USING (
#     SELECT A.col1, B.col2 
#     FROM (SELECT col1 FROM source_table1) A 
#     JOIN (SELECT col2 FROM source_table2) B ON A.ref = B.ref
# ) AS src
# ON TT.col1 = src.col1
# WHEN MATCHED THEN UPDATE SET TT.col2 = src.col2;

# SELECT * FROM (SELECT col1 FROM tableX) aliasX;
# '''

def parse(query):
    refined_connection = []
    query_list = []
    for q in query:
        q = q[0]
        if q:
            result = extract(q)
            if result:
                refined_connection.append(result)
                query_list.append(q)

    populate_table_relation(refined_connection,query_list)



    
# def parse(query):
#     query_list = sqlparse.split(query)
#     refined_connection = []

#     for q in query_list:
#         q = q.strip()
#         if q:
#             result = extract(q)
#             if result:
#                 refined_connection.append(result)