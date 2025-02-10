from sqlalchemy.orm import sessionmaker
from metadata_ingest.model import model, Table, table_relation

def populate_table_relation(lineage_data, query_list):
    engine = model()

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Mapping table relationships to their queries
        query_index = 0

        for entry in lineage_data:
            for target_table, source_tables in entry.items():
                target_table_entry = session.query(Table).filter_by(tab_name=target_table).first()
                if not target_table_entry:
                    print(f"Target table '{target_table}' not found. Skipping.")
                    continue

                target_tab_id = target_table_entry.tab_id

                for source_table in source_tables:
                    source_table_entry = session.query(Table).filter_by(tab_name=source_table).first()
                    if not source_table_entry:
                        print(f"Source table '{source_table}' not found. Skipping.")
                        continue

                    source_tab_id = source_table_entry.tab_id

                    # Extract the corresponding query if available
                    query = query_list[query_index] if query_index < len(query_list) else None
                    query_index += 1

                    existing_relation = session.query(table_relation).filter_by(
                        target_tab_id=target_tab_id,
                        source_tab_id=source_tab_id
                    ).first()

                    if not existing_relation:
                        relation = table_relation(
                            target_tab_id=target_tab_id,
                            source_tab_id=source_tab_id,
                            query=query  # Store the query in the table_relation entry
                        )
                        session.add(relation)
                        print(f"Added relation: {source_table} -> {target_table} with query: {query}")
        
        session.commit()
        print("Table relations populated successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()
