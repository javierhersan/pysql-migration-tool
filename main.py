import azure.functions as func
import azure.durable_functions as df
from datetime import datetime
from pandas import DataFrame 
import pandas as pd 
from pathlib import Path

import repositories.database_repository as database_repository

try:
    table_name = '[DWH].[FACT_LRL]' # Table to syncronize [SCHEMA].[TABLE]
    schema, table = database_repository.extract_schema_and_table(table_name)

    # Azure
    origin_connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:server;PORT=1433;DATABASE=database;UID=username;PWD=password;TrustServerCertificate=yes;' 
    # Azure sync
    destination_connection_string_1 = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:server;PORT=1433;DATABASE=database;UID=username;PWD=password;TrustServerCertificate=yes;' 
    
    BATCH_SIZE = 50000
    row_count = database_repository.count(origin_connection_string, schema, table)
    offset = 0
    destination_column_typos = database_repository.get_column_typos(destination_connection_string_1, table_name)

    print(f"Start: {datetime.now()}")

    while offset < row_count:

        batch_data = database_repository.select(origin_connection_string, schema, table, BATCH_SIZE)

        batch_data_filled = database_repository.fill_nulls(batch_data, destination_column_typos)

        database_repository.insert(destination_connection_string_1, schema, table, batch_data_filled)

        database_repository.delete(origin_connection_string, schema, table, BATCH_SIZE)
        
        offset += BATCH_SIZE
        print(f"Running: {offset}/{row_count}")

    print(f"End: {datetime.now()}")

except Exception as e: 
    print('Failure')