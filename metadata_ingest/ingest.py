from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from metadata_ingest.model import model, Database, Schema, Table, View, StoredProcedure

def fetch_metadata_and_insert(source_engine, db_name):
    engine = model()

    Session = sessionmaker(bind=engine)
    session = Session()

    metadata_query = '''
        SELECT s.name AS SchemaName,
               t.name AS ObjectName,
               'table' AS ObjectType
        FROM sys.tables t
        INNER JOIN sys.schemas s 
            ON t.schema_id = s.schema_id
        UNION 
        SELECT s.name AS SchemaName,
               v.name AS ObjectName,
               'views' AS ObjectType
        FROM sys.views v
        INNER JOIN sys.schemas s 
            ON v.schema_id = s.schema_id
        UNION 
        SELECT s.name AS SchemaName,
               p.name AS ObjectName,
               'sp' AS ObjectType
        FROM sys.procedures p
        INNER JOIN sys.schemas s 
            ON p.schema_id = s.schema_id
    '''

    source_conn = source_engine.connect()

    try:
        source_conn = source_conn.connect()
        result = source_conn.execute(text(metadata_query))

        db_entry = session.query(Database).filter_by(db_name=db_name).first()
        if not db_entry:
            db_entry = Database(db_name=db_name)
            session.add(db_entry)
            session.commit()

        schema_map = {}

        for row in result:
            schema_name = row[0]
            object_name = row[1]
            object_type = row[2]

            if schema_name not in schema_map:
                schema_entry = session.query(Schema).filter_by(schema_name=schema_name, db_id=db_entry.db_id).first()
                if not schema_entry:
                    schema_entry = Schema(schema_name=schema_name, database=db_entry)
                    session.add(schema_entry)
                    session.commit()
                schema_map[schema_name] = schema_entry

            schema_entry = schema_map[schema_name]

            if object_type == 'table':
                if not session.query(Table).filter_by(tab_name=object_name, schema_id=schema_entry.schema_id).first():
                    session.add(Table(tab_name=object_name, schema=schema_entry))
            elif object_type == 'views':
                if not session.query(View).filter_by(v_name=object_name, schema_id=schema_entry.schema_id).first():
                    session.add(View(v_name=object_name, schema=schema_entry))
            elif object_type == 'sp':
                if not session.query(StoredProcedure).filter_by(sp_name=object_name, schema_id=schema_entry.schema_id).first():
                    session.add(StoredProcedure(sp_name=object_name, schema=schema_entry))

        session.commit()
    finally:
        source_conn.close()
        session.close()
