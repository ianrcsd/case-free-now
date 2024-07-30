from typing import Dict

import pendulum
from airflow import DAG
from airflow.decorators import dag, task


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_2_stream() -> DAG:
    """DAG that streams record from an artifical API and stores them in a DB"""

    @task
    def source_data(**op_kwargs) -> Dict[str, any]:
        """Read file based on DS from list of transactions files convert to binary format and store in tmp file"""
        raise NotImplementedError

    @task
    def process_data(table: Dict[str, any]) -> None:
        """Read tmp binary file and apply a schema on it to validate data.
        Sum the values by key and then return the key with the 3rd largest result for the given date.

        return: 3rd largest result
        """
        raise NotImplementedError

    process_data(source_data())


dag = task_2_stream()
