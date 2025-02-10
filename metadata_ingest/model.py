from sqlalchemy import (
    create_engine, Column, Integer, String, ForeignKey, Table
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from lineage_query_extract.db import MSSQLConnection

Base = declarative_base()

class Database(Base):
    __tablename__ = 'databases'
    
    db_id = Column(Integer, primary_key=True, autoincrement=True)
    db_name = Column(String(100), nullable=False, unique=True)

    schemas = relationship("Schema", back_populates="database")


class Schema(Base):
    __tablename__ = 'schemas'
    
    schema_id = Column(Integer, primary_key=True, autoincrement=True)
    schema_name = Column(String(100), nullable=False)
    db_id = Column(Integer, ForeignKey('databases.db_id', ondelete="CASCADE"))

    database = relationship("Database", back_populates="schemas")
    tables = relationship("Table", back_populates="schema")
    views = relationship("View", back_populates="schema")
    stored_procedures = relationship("StoredProcedure", back_populates="schema")


class Table(Base):
    __tablename__ = 'tables'
    
    tab_id = Column(Integer, primary_key=True, autoincrement=True)
    tab_name = Column(String(100), nullable=False)
    schema_id = Column(Integer, ForeignKey('schemas.schema_id', ondelete="CASCADE"))

    schema = relationship("Schema", back_populates="tables")

class table_relation(Base):
    __tablename__ = 'table_relation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    target_tab_id = Column(Integer, ForeignKey('tables.tab_id'),nullable=False)
    source_tab_id = Column(Integer, ForeignKey('tables.tab_id'),nullable=False)
    query = Column(String, nullable=True) 

class View(Base):
    __tablename__ = 'views'
    
    v_id = Column(Integer, primary_key=True, autoincrement=True)
    v_name = Column(String(100), nullable=False)
    schema_id = Column(Integer, ForeignKey('schemas.schema_id', ondelete="CASCADE"))

    schema = relationship("Schema", back_populates="views")


class StoredProcedure(Base):
    __tablename__ = 'stored_procedures'
    
    sp_id = Column(Integer, primary_key=True, autoincrement=True)
    sp_name = Column(String(100), nullable=False)
    schema_id = Column(Integer, ForeignKey('schemas.schema_id', ondelete="CASCADE"))

    schema = relationship("Schema", back_populates="stored_procedures")

def model():
    meta_data = MSSQLConnection('localhost','meta_data','lin','sql')
    engine = meta_data.connect()
    Base.metadata.create_all(engine)

    return engine