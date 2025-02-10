from lineage_query_extract.db import MSSQLConnection
from sqlalchemy import text
from lineage_query_extract.parse import parse

def main(d) -> None:
    mssql = MSSQLConnection('localhost',d,'lin','sql')
    mssql = mssql.connect()
    with mssql.connect() as conn:
        res = conn.execute(text('''
                                    SELECT 
                                            m.definition AS QueryText,
                                            NULL AS last_execution_time
                                        FROM sys.dm_exec_procedure_stats dm
                                        INNER JOIN sys.sql_modules m
                                        ON dm.object_id = m.object_id
                                        UNION
                                        SELECT 
                                            LTRIM(RTRIM(
                                                SUBSTRING(
                                                    qt.[text],
                                                    (qs.statement_start_offset / 2) + 1,
                                                    (
                                                        (CASE qs.statement_end_offset 
                                                            WHEN -1 THEN DATALENGTH(qt.[text])
                                                            ELSE qs.statement_end_offset 
                                                        END - qs.statement_start_offset) / 2
                                                    ) + 1
                                                )
                                            )) AS QueryText,
                                            qs.last_execution_time
                                        FROM sys.dm_exec_query_stats AS qs
                                        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
                                        WHERE qt.[text] NOT LIKE '%sys%'
                                        ORDER BY last_execution_time DESC;'''))
        parse(res.fetchall())

if __name__ == '__main__':
    main()