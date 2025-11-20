import pandas as pd
from sqlalchemy import create_engine,exc
import os
import psycopg2

# Configuration of the database port is 5433 for me but if you guys run it from your computer it will be different
DB_USER = 'postgres'
DB_PASS = 'postgres'
DB_HOST = 'localhost'
DB_NAME = 'municipal_bonds' # <-- FIX 1: Changed to 'municipal_bonds' (plural) to match your PgAdmin database
DB_PORT = '5433'

# Connect to SQLALCHEMY
CONNECTION_STRING = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'


# CONFIGURE csv files
DATA_PATH = './'

# I found out order is important here...
# Parent tables like issuers need to load in before child tables like bonds
TABLE_FILES ={
    'issuers': 'issuers.csv',
    'bond_purposes':'bond_purposes.csv',
    'macro_economic_data':'economic_indicators.csv', # <-- FIX 2: Key changed to 'macro_economic_data' to match schema.sql
    'bonds': 'bonds.csv',
    'credit_ratings': 'credit_ratings.csv', # <-- FIX 3: Key changed to 'credit_ratings' to match schema.sql
    'trades':'trades.csv'
}


# Translates csv column names to the database schema column names. Helps for headers that don't match perfect to database
COLUMN_RENAME_MAP = {
    'state': 'state_code',
    'unemployment_rate': 'unemployment_rate_pct',
    'treasury_10yr': 'treasury_10yr_rate_pct',
    'treasury_20yr': 'treasury_20yr_rate_pct',
    'vix_index': 'vix_index_num',
    'population': 'population_num',
    'tax_base_millions': 'tax_base_millions_num',
    'coupon_rate': 'coupon_rate_pct',
    'face_value': 'face_value_usd',
    'duration': 'duration_years',
    'trade_price': 'trade_price_usd',
    'yield': 'yield_pct',
    'quantity': 'quantity_num',
    'rating_agency': 'rating_agency_name',
    'rating': 'rating_code'
}


# ---Function to extract, transform , and load data. **---Note for self: Can use this function in future projects----***
# Steps for my group or anyone to recreate:
# 1) Extract which constructs the ful;l file path and reads the CSV into a pandas df
# 2) First part of transformation. Fetches target tables schema from pdgadmin (postgres)
# 3) Then the next transformation is renaming the columns in new df columns using COLUMN_RENAME_MAP to match the schema
# 4) Last transformation is filtering the df to only include columns that exist in the target table.
# 5) Then using pandas to_sql method to append data into database

### Notes for self for future use. Arguments are:
### db_engine(sqlalchemy.engine.Engine): The active SQLAlchemy engine connection.
### table_name (str): The name of the database table to load into.
### file_name (str): The name of the CSV file to read.


def load_table(db_engine, table_name, file_name):
    file_path = os.path.join(DATA_PATH, file_name)
    print(f"[INFO] Processing {table_name} ({file_name})")

    try:
        csv_data = pd.read_csv(file_path)
        print(f"  > Found {len(csv_data)} rows in {file_name}")

        schema_columns = pd.read_sql(f'SELECT * FROM {table_name} LIMIT 0', db_engine).columns
        data_to_load = csv_data.rename(columns=COLUMN_RENAME_MAP)
        data_to_load = data_to_load[data_to_load.columns.intersection(schema_columns)]
        final_cols_count = len(data_to_load.columns)

        print(f"  > Matched {final_cols_count} CSV columns to the '{table_name}' table schema.")

        if final_cols_count == 0:
            print(f"  [WARN] No columns matched the schema for {table_name}. Skipping load.")
            return

        data_to_load.to_sql(table_name, db_engine, if_exists='append', index=False)

        print(f"  > Successfully loaded {len(data_to_load)} rows into '{table_name}'.")

    except FileNotFoundError as file_error:
        print(f"  [ERROR] File not found at {file_path}. Skipping.")
        print(f"    Details: {file_error}")
    except exc.SQLAlchemyError as db_error:
        print(f"  [ERROR] Database error loading {table_name}.")
        print(f"    This often happens if data violates a CHECK or FOREIGN KEY constraint.")
        print(f"    Details: {db_error}")
    except Exception as general_error:
        print(f"  [ERROR] An unexpected error occurred while loading {table_name}: {general_error}")
        print("    Please check column names, data types, and file paths.")


# ---- Main function that creates database engine verifies the connection to database and iterates through the table files dictionary in the correct order
# First is creates the database engine, second it verifies the database connection and lastly, it iterates through the correct order of the table_files dict
def main():
    print("Beginning data load process...")
    try:
        engine = create_engine(CONNECTION_STRING)
        with engine.connect() as connection:
            print(f"[INFO] Successfully connected to database '{DB_NAME}' at {DB_HOST}.")
    except exc.OperationalError as conn_error:
        print(f"[FATAL] Could not connect to the database.")
        print("  Please check your database credentials (user, pass, host, port) and ensure PostgreSQL is running.")
        print(f"  Error details: {conn_error}")
        return
    except Exception as e:
        print(f"[FATAL] An unknown connection error occurred: {e}")
        return

    print("Beginning table loading process...")
    for table, file in TABLE_FILES.items():
        load_table(engine, table, file)
    print("Data load complete. WOOT WOOT!")
if __name__ == "__main__":
    main()