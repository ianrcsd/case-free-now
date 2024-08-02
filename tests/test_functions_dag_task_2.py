import json
import os
from tempfile import NamedTemporaryFile

import pandas as pd
import pytest

from common.task2_funcs import (
    convert_to_avro,
    get_largest_key,
    read_avro_file,
    read_csv_file_to_df,
)
from config import DATA_PATH
from dags.task_2_stream import _generate_file_path


@pytest.fixture
def sample_csv_file():
    with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        tmp_file.write(b"key,value\n1,A\n2,B\n")
        tmp_file_path = tmp_file.name
    yield tmp_file_path
    os.remove(tmp_file_path)


@pytest.fixture
def sample_csv_dict():
    with NamedTemporaryFile(
        delete=False, suffix=".csv"
    ) as tmp_file1, NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file2:
        tmp_file1.write(b"1,A\n2,B\n")
        tmp_file2.write(b"3,C\n4,D\n")
        tmp_file1_path = tmp_file1.name
        tmp_file2_path = tmp_file2.name
    yield {"2021-01-01": tmp_file1_path, "2021-01-02": tmp_file2_path}
    os.remove(tmp_file1_path)
    os.remove(tmp_file2_path)


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({"key": [1, 2], "value": ["A", "B"]})


@pytest.fixture
def sample_dataframe_get_largest():
    return pd.DataFrame(
        {
            "key": [1, 2, 2, 3, 3, 3, 4, 4, 4, "A", 5],
            "value": [10, 20, 20, 30, 30, 30, 40, 40, 40, "A", None],
        }
    )


@pytest.fixture
def sample_avro_schema():
    return {
        "type": "record",
        "name": "TestRecord",
        "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "string"}],
    }


def test_read_csv_file_to_df_single_file(sample_csv_file):
    df = read_csv_file_to_df(sample_csv_file)
    assert not df.empty
    assert list(df.columns) == ["key", "value"]
    assert df.shape == (2, 2)


def test_read_csv_file_to_df_dict(sample_csv_dict):
    df = read_csv_file_to_df(sample_csv_dict)
    assert not df.empty
    assert list(df.columns) == ["key", "value", "date"]
    assert df.shape == (4, 3)


def test_convert_to_avro(sample_dataframe, sample_avro_schema):
    avro_path = convert_to_avro(sample_dataframe, sample_avro_schema)
    assert os.path.exists(avro_path)


def test_read_avro_file(sample_dataframe, sample_avro_schema):
    avro_path = convert_to_avro(sample_dataframe, sample_avro_schema)
    df = read_avro_file(avro_path)
    assert not df.empty
    assert list(df.columns) == ["key", "value"]
    assert df.shape == (2, 2)


def test_get_largest_key_valid(sample_dataframe_get_largest):
    result = get_largest_key(sample_dataframe_get_largest, 3)
    assert result == 2


def test_get_largest_key_not_enough_data(sample_dataframe_get_largest):
    with pytest.raises(
        ValueError, match="Not enough data to find the 9th largest result"
    ):
        get_largest_key(sample_dataframe_get_largest, 9)


def test_get_largest_key_no_data():
    empty_df = pd.DataFrame({"key": [], "value": []})
    with pytest.raises(ValueError, match="No data available"):
        get_largest_key(empty_df, 1)


def test_generate_file_path():
    result = _generate_file_path("2021-01-01")
    expected = {"2021-01-01": f"{DATA_PATH}/transactions_2021-01-01.csv"}
    assert result == expected

    result = _generate_file_path(["2021-01-01", "2021-01-02"])
    expected = {
        "2021-01-01": f"{DATA_PATH}/transactions_2021-01-01.csv",
        "2021-01-02": f"{DATA_PATH}/transactions_2021-01-02.csv",
    }
    assert result == expected
