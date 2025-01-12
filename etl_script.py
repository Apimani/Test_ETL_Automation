import pandas as pd
from sqlalchemy import create_engine

# Database connection
def get_db_connection(user, password, host, port, database):
    """
    Establishes a connection to the MySQL database using SQLAlchemy.
    """
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        return engine
    except Exception as e:
        raise ConnectionError(f"Failed to connect to the database: {e}")

# Extract data
def extract_data(engine, table_name):
    """
    Extracts data from a specified table in the database.
    """
    try:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, con=engine)
    except Exception as e:
        raise ValueError(f"Failed to extract data from table '{table_name}': {e}")

# Transform data
def transform_data(data):
    """
    Transforms the extracted data. Example: Adds a 'total_compensation' column.
    """
    try:
        if 'salary' in data.columns and 'bonus' in data.columns:
            data['total_compensation'] = data['salary'] + data['bonus']
        else:
            raise KeyError("Columns 'salary' and/or 'bonus' are missing in the data.")
        return data
    except Exception as e:
        raise ValueError(f"Failed to transform data: {e}")

# Load data
def load_data(engine, data, target_table):
    """
    Loads the transformed data into a specified table in the database.
    """
    try:
        data.to_sql(target_table, con=engine, if_exists='replace', index=False)
    except Exception as e:
        raise ValueError(f"Failed to load data into table '{target_table}': {e}")
