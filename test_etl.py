import pytest
import pandas as pd
from etl_script import extract_data, transform_data, load_data, get_db_connection

# Database credentials (update as needed)
DB_USER = "root"
DB_PASSWORD = "Tiger"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "mpmani"

SOURCE_TABLE = "employees_tgt"
TARGET_TABLE = "employees_tgt001"

@pytest.fixture
def db_engine():
    """
    Provides a database engine fixture for connecting to the database.
    """
    return get_db_connection(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

@pytest.fixture
def sample_data():
    """
    Provides sample data for testing the ETL process.
    """
    return pd.DataFrame({
        "employee_id": [1, 2, 3],
        "first_name": ["John", "Jane", "Mike"],
        "last_name": ["Doe", "Smith", "Johnson"],
        "department": ["Engineering", "Marketing", "Finance"],
        "salary": [75000, 65000, 60000],
        "bonus": [7500.0, 6500.0, 6000.0],
        "join_date": ["2020-01-15", "2019-07-10", "2021-03-12"]
    })

def test_extract_data(db_engine, sample_data):
    """
    Tests the extract_data function.
    """
    # Load sample data into the source table
    sample_data.to_sql(SOURCE_TABLE, con=db_engine, if_exists='replace', index=False)

    # Extract data
    extracted_data = extract_data(db_engine, SOURCE_TABLE)

    # Validate extracted data
    assert not extracted_data.empty, "Extracted data is empty."
    assert len(extracted_data) == len(sample_data), "Mismatch in row count of extracted data."

def test_transform_data(sample_data):
    """
    Tests the transform_data function.
    """
    transformed_data = transform_data(sample_data)

    # Validate transformed data
    assert 'total_compensation' in transformed_data.columns, "Missing 'total_compensation' column."
    assert (transformed_data['total_compensation'] == (transformed_data['salary'] + transformed_data['bonus'])).all(), \
        "Incorrect 'total_compensation' values."

def test_load_data(db_engine, sample_data):
    """
    Tests the load_data function.
    """
    transformed_data = transform_data(sample_data)

    # Load transformed data into the target table
    load_data(db_engine, transformed_data, TARGET_TABLE)

    # Validate data loaded into target table
    loaded_data = pd.read_sql(f"SELECT * FROM {TARGET_TABLE}", con=db_engine)
    assert not loaded_data.empty, "Loaded data is empty."
    assert len(loaded_data) == len(transformed_data), "Mismatch in row count of loaded data."

def test_end_to_end_etl(db_engine, sample_data):
    """
    Tests the entire ETL process end-to-end.
    """
    # Step 1: Load sample data into the source table
    sample_data.to_sql(SOURCE_TABLE, con=db_engine, if_exists='replace', index=False)

    # Step 2: Extract data
    extracted_data = extract_data(db_engine, SOURCE_TABLE)

    # Step 3: Transform data
    transformed_data = transform_data(extracted_data)

    # Step 4: Load transformed data into the target table
    load_data(db_engine, transformed_data, TARGET_TABLE)

    # Validate data in target table
    loaded_data = pd.read_sql(f"SELECT * FROM {TARGET_TABLE}", con=db_engine)
    assert not loaded_data.empty, "ETL process failed: Target table is empty."
    assert 'total_compensation' in loaded_data.columns, "Missing 'total_compensation' column in target table."
