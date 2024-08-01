import json
import os
import tempfile

import pandas as pd
import pytest
from airflow.models import Connection
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.settings import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Field, SQLModel, create_engine

from common.task1_funcs import (
    convert_to_avro,
    convert_to_table_to_data_frame,
    ingest_bronze_to_silver_func,
    ingest_silver_to_gold_func,
    ingest_to_bronze_func,
    read_avro_file,
)

# Sample data for testing
sample_df = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "country_code": ["US", "GB", "FR"],
    }
)

# Sample Avro schema
sample_avro_schema = {
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "country_code", "type": "string"},
    ],
}


def test_convert_to_avro():
    avro_path = convert_to_avro(sample_df, sample_avro_schema)
    assert os.path.exists(avro_path)
    os.remove(avro_path)


def test_read_avro_file():
    avro_path = convert_to_avro(sample_df, sample_avro_schema)
    df = read_avro_file(avro_path)
    assert not df.empty
    assert df.shape == (3, 3)
    os.remove(avro_path)


class MockTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    name: str


@pytest.fixture
def connection():
    # Set up an in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with engine.connect() as connection:
        yield connection


def test_convert_to_table_to_data_frame(connection):
    # Perform the function test
    connection.execute(
        "INSERT INTO mocktable (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
    )
    df = convert_to_table_to_data_frame(connection, MockTable)

    # Define the expected DataFrame
    expected_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    # Assert that the result matches the expected DataFrame
    pd.testing.assert_frame_equal(df, expected_df)
