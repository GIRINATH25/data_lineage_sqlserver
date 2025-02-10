from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy_utils import create_database,database_exists

class BaseDBConnection:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.engine = None

class MSSQLConnection(BaseDBConnection):
    def connect(self):
        # connection_url = URL(
        #     drivername="mssql+pyodbc",
        #     username=self.username,
        #     password=self.password,
        #     host=self.server,
        #     database=self.database,
        #     port="1433",
        #     query={"driver": "ODBC Driver 17 for SQL Server", "TrustServerCertificate": "yes"},
        # )

        connection_url = URL.create(
            drivername="mssql+pyodbc",
            host=self.server,
            database=self.database,
            query={
            "driver": "ODBC Driver 17 for SQL Server",
            "Trusted_Connection": "yes"
            }
        )

        try:

            engine = create_engine(connection_url)
            self.engine = engine
            # print(self.engine)
            if not database_exists(self.engine.url):
                create_database(self.engine.url)
            return self.engine
        
        except Exception as e:
            print("error on create db")