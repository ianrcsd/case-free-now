import os
from unittest.mock import patch

import pandas as pd
import pendulum
import pytest
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State

from dags import task_2_stream
from dags.task_2_stream import _generate_file_path

PATH_DATA = os.getenv("DATA_PATH")


@pytest.fixture
def dagbag():
    return DagBag()


@patch("task_2_stream.read_csv_file_to_df")
def test_source_data_task(mock_read_csv, dagbag):
    mock_read_csv.return_value = pd.DataFrame(
        {"key": ["a", "b"], "value": [1, 2], "date": ["2021-01-01", "2021-01-02"]}
    )

    dag = dagbag.get_dag(dag_id="task_2_stream")
    task = dag.get_task("source_data")
    ti = TaskInstance(task=task, execution_date=pendulum.datetime(2021, 1, 1, tz="UTC"))
    ti.run()

    assert ti.state == State.SUCCESS


def test_generate_file_path():
    result = _generate_file_path("2021-01-01")
    expected = {"2021-01-01": f"{PATH_DATA}/transactions_2021-01-01.csv"}
    assert result == expected

    result = _generate_file_path(["2021-01-01", "2021-01-02"])
    expected = {
        "2021-01-01": f"{PATH_DATA}/transactions_2021-01-01.csv",
        "2021-01-02": f"{PATH_DATA}/transactions_2021-01-02.csv",
    }
    assert result == expected


def test_validation_wrong_data():
    # Example of wrong data
    wrong_data = pd.DataFrame(
        {
            "key": ["a", "b"],
            "value": ["one", "two"],  # should be int
            "date": ["2021-01-01", "2021-01-02"],
        }
    )

    with pytest.raises(ValueError):
        # Assuming there's a validation function to check the data
        validate_data(wrong_data)


if __name__ == "__main__":
    pytest.main()
