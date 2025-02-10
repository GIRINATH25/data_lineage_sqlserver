from flask import Flask, render_template, request
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from metadata_ingest.model import Database, Schema, Table, View, StoredProcedure
from flask import jsonify
import metadata_ingest.__main__ as meta_ingest
import lineage_query_extract.__main__ as lineage_ingest

app = Flask(__name__)
DATABASE_URI = "mssql+pyodbc://localhost/meta_data?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()


@app.route("/")
def index():
    with engine.connect() as conn:
        res = conn.execute(text('''
                        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'databases'
                                '''))
        something = res.fetchall()
        if not something:
            return render_template("index.html", databases=[])
    databases = session.query(Database).all()
    return render_template("index.html", databases=databases)


@app.route("/database/<int:db_id>")
def database(db_id):
    database = session.query(Database).filter_by(db_id=db_id).first()
    schemas = database.schemas
    return render_template("database.html", database=database, schemas=schemas)


@app.route("/schema/<int:schema_id>")
def schema(schema_id):
    schema = session.query(Schema).filter_by(schema_id=schema_id).first()
    tables = schema.tables
    views = schema.views
    stored_procedures = schema.stored_procedures
    return render_template("schema.html", schema=schema, tables=tables, views=views, stored_procedures=stored_procedures)


@app.route("/table_relation")
def table_relation():
    relations = session.execute("""
        SELECT t1.tab_name AS TargetTable, t2.tab_name AS SourceTable
        FROM table_relation tr
        JOIN tables t1 ON tr.target_tab_id = t1.tab_id
        JOIN tables t2 ON tr.source_tab_id = t2.tab_id
    """).fetchall()
    return render_template("objects.html", relations=relations)

@app.route("/table/<int:tab_id>")
def table_lineage(tab_id):
    """
    Fetch lineage data recursively for the given table and pass it to the template for visualization.
    """
    lineage_query = text("""
        WITH RecursiveCTE AS (
            SELECT 
                target_tab_id,
                source_tab_id,
                query,
                1 AS Level
            FROM 
                dbo.table_relation
            WHERE 
                target_tab_id = :tab_id
            UNION ALL
            SELECT 
                t.target_tab_id,
                t.source_tab_id,
                t.query,
                r.Level + 1
            FROM 
                dbo.table_relation t
            INNER JOIN 
                RecursiveCTE r ON t.target_tab_id = r.source_tab_id
        )
        SELECT 
            target_tab_id, source_tab_id, query, Level 
        FROM 
            RecursiveCTE;
    """)

    result = session.execute(lineage_query, {"tab_id": tab_id}).fetchall()

    # Fetch table names and map to IDs
    table_name_map = {table.tab_id: table.tab_name for table in session.query(Table).all()}

    nodes = []
    links = []

    for row in result:
        source_name = table_name_map.get(row.source_tab_id, "Unknown")
        target_name = table_name_map.get(row.target_tab_id, "Unknown")
        query = row.query if row.query else "No query available"

        # Add nodes if not already present
        if source_name not in [n["name"] for n in nodes]:
            nodes.append({"name": source_name})
        if target_name not in [n["name"] for n in nodes]:
            nodes.append({"name": target_name})

        # Add links with query details
        links.append({"source": source_name, "target": target_name, "level": row.Level, "query": query})
    # print(links)
    return render_template("table_lineage.html", nodes=nodes, links=links)

@app.route("/ingestdb")
def ingestdb():
    db_name = request.args.get("db_name")
    if not db_name:
        return "Database name is required.", 400
    try:
        meta_ingest.main(db_name)
        return f"Database '{db_name}' ingested successfully."
    except Exception as e:
        return f"Failed to ingest database '{db_name}': {str(e)}"

@app.route("/lineageingest")
def lineageingest():
    db_name = request.args.get("db_name")
    if not db_name:
        return "Database name is required.", 400
    try:
        lineage_ingest.main(db_name)
        return "Lineage ingested successfully."
    except Exception as e:
        return f"Failed to ingest lineage: {str(e)}"



if __name__ == "__main__":
    app.run(debug=True)
