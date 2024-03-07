import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine


def write_data_postgres(dataframe: pd.DataFrame, table_exists_action: str = 'append') -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.DataFrame): The dataframe to dump into the DBMS engine
        - table_exists_action (str): Action to take if the table already exists ('fail', 'replace', 'append')

    Returns:
        - bool: True if the connection to the DBMS and the dump to the DBMS is successful, False if either
          execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )

    success = True  # Initialize success outside the try-except block

    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect() as connection:
            print("Connection successful! Processing dataframe")
            dataframe.to_sql(db_config["dbms_table"], connection, index=False, if_exists=table_exists_action)

    except Exception as e:
        success = False
        print(f"Error connecting to the database or writing data: {e}")

    return success

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite its columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame): The dataframe columns to change

    Returns:
        - pd.DataFrame: The changed dataframe with lowercase column names.
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    parquet_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        try:
            with pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow') as parquet_df:
                clean_column_name(parquet_df)
                if not write_data_postgres(parquet_df, table_exists_action='append'):
                    continue
        except Exception as e:
            print(f"Error reading parquet file '{parquet_file}': {e}")

if __name__ == '__main__':
    sys.exit(main())