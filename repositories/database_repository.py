import datetime
import re
import pandas as pd
import pyodbc
import sqlalchemy
import os
from pandas import DataFrame 
import sqlalchemy
from sqlalchemy import create_engine

# ----------------------------------------------- #
# -------------------- Database ----------------- #
# ----------------------------------------------- #

def count(conn_str: str, schema:str, table:str) -> DataFrame:
    try:
        conn = pyodbc.connect(conn_str)
        sql = f"SELECT COUNT(*) AS COUNT FROM [{schema}].[{table}]"
        count = pd.read_sql(sql,conn).iloc[0]['COUNT']
        conn.commit()
        conn.close()
        return count
    except Exception as e: 
        if 'conn' in locals():
            conn.close()

def select(conn_str: str, schema:str, table:str, batch_size:int, offset:int = 0) -> DataFrame:
    try:
        conn = pyodbc.connect(conn_str)
        # sql = f"SELECT * FROM [{schema}].[{table}] ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        sql = f"SELECT TOP({batch_size}) * FROM [{schema}].[{table}] ORDER BY 1"
        data = pd.read_sql(sql,conn)
        conn.commit()
        conn.close()
        return data
    except Exception as e: 
        if 'conn' in locals():
            conn.close()

def insert(conn_str: str, schema:str, table:str, data: DataFrame) -> None:
    try:
        connection_url = sqlalchemy.engine.URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        engine = create_engine(connection_url, fast_executemany=True)
        conn = engine.connect()
        data.to_sql(table, schema = schema, con = conn, if_exists='append', index=False, chunksize=1000)
        conn.close()
        engine.dispose()
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        if 'engine' in locals():
            engine.dispose()

def delete(conn_str: str, schema:str, table:str, batch_size:int) -> None:
    try:
        conn = pyodbc.connect(conn_str)
        cursor=conn.cursor()
        cursor.execute(f"DELETE TOP({batch_size}) FROM [{schema}].[{table}]")
        conn.commit()
        conn.close()
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
    
def extract_schema_and_table(table_name: str) -> tuple[str, str]:
    try:
        match = re.match(r'\[([^]]+)\]\.\[([^]]+)\]', table_name)
        if match:
            schema = match.group(1)
            table = match.group(2)
            return schema, table
        else:
            return None, None
    except Exception as e: 
        print()


def fill_nulls(data:DataFrame, column_typos:DataFrame) -> DataFrame:
    try:
        data_columns = data.columns
        db_columns = column_typos['COLUMN_NAME'].unique()

        for col in db_columns:
            db_field = column_typos[column_typos['COLUMN_NAME']==col]
            if not db_field.empty:
                typo = db_field.iloc[0]['DATA_TYPE']
                nullable = db_field.iloc[0]['IS_NULLABLE']
                if nullable == "NO":
                    if typo=='int':
                        if col.lower() in map(str.lower, data_columns):
                            data_column = next(column for column in data_columns if column.lower() == col.lower())
                            data[data_column] = data[data_column].fillna(value=0)
                        else:
                            data[col] = 0
                    elif typo=='decimal':
                        if col.lower() in map(str.lower, data_columns):
                            data_column = next(column for column in data_columns if column.lower() == col.lower())
                            data[data_column] = data[data_column].fillna(value=0.0)
                        else:
                            data[col] = 0.0
                    elif typo=='date':
                        default_date = pd.to_datetime('01/01/1900 0:00').normalize()
                        if col.lower() in map(str.lower, data_columns):
                            data_column = next(column for column in data_columns if column.lower() == col.lower())
                            data[data_column] = data[data_column].fillna(value=default_date)
                        else:
                            data[col] = default_date
                    elif typo=='nvarchar' or typo=='varchar' or typo=='char':
                        if col.lower() in map(str.lower, data_columns):
                            data_column = next(column for column in data_columns if column.lower() == col.lower())
                            data[data_column] = data[data_column].fillna(value="")
                        else:
                            data[col] = ""
        return data
    except Exception as e: 
        print()
    

def get_column_typos(conn_str: str, table_name:str) -> DataFrame:
    try:
        conn = pyodbc.connect(conn_str)
        match = re.match(r'\[([^]]+)\]\.\[([^]]+)\]', table_name)
        if match:
            schema = match.group(1)
            table = match.group(2)
            sql =   """
                        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
                    """
            data_validation_rules = pd.read_sql(sql,conn, params=(schema,table))
            conn.commit()
        conn.close()
        return data_validation_rules
    except Exception as e: 
        print()
