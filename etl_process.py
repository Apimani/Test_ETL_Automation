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
        query = f"SELECT * FROM employees_tgt"
        return pd.read_sql(query, con=engine)
    except Exception as e:
        raise ValueError(f"Failed to extract data from table 'employees_tgt': {e}")

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
        raise ValueError(f"Failed to load data into table 'employees_tgt001': {e}")

# Main execution (for testing purposes)
if __name__ == "__main__":
    # Database credentials
    DB_USER = "root"
    DB_PASSWORD = "Tiger"
    DB_HOST = "localhost"
    DB_PORT = "3306"
    DB_NAME = "mpmani"

    # Table names
    SOURCE_TABLE = "employees_tgt"
    TARGET_TABLE = "employees_tgt001"

    # Step 1: Get database connection
    engine = get_db_connection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    # Step 2: Extract data
    extracted_data = extract_data(engine, SOURCE_TABLE)
    print("Extracted Data:")
    print(extracted_data)

    # Step 3: Transform data
    transformed_data = transform_data(extracted_data)
    print("\nTransformed Data:")
    print(transformed_data)

    # Step 4: Load data
    load_data(engine, transformed_data, TARGET_TABLE)
    print(f"\nData successfully loaded into the table '{TARGET_TABLE}'.")
