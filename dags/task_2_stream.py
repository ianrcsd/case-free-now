import logging
import os
from typing import Any, Dict, List, Union

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from common.task2_funcs import (
    convert_to_avro,
    get_largest_key,
    read_avro_file,
    read_csv_file_to_df,
)
from config import DATA_PATH

logger = logging.getLogger("airflow.task")


AVRO_SCHEMA = {
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "key", "type": "string"},
        {"name": "value", "type": "int"},
        {"name": "date", "type": "string"},
    ],
}


def _generate_file_path(ds: Union[list, str]) -> dict:
    if isinstance(ds, str):
        ds = [ds]
    return {date: os.path.join(DATA_PATH, f"transactions_{date}.csv") for date in ds}


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_2_stream() -> DAG:
    """DAG that streams record from an artificial API and stores them in a DB"""

    @task
    def source_data(**op_kwargs) -> Dict[str, Any]:
        """Read file based on DS from list of transactions convert to binary format, and store in tmp file"""

        ds = op_kwargs["ds"]
        logger.info(f"Generating file path for date: {ds}")
        file_path = _generate_file_path(ds)
        logger.info(f"Reading CSV file from path: {file_path[ds]}")
        try:
            data_df = read_csv_file_to_df(file_path)
        except FileNotFoundError as e:
            logger.error(f"{e}")
        logger.info(f"Convert Data to AVRO format")
        try:
            avro_path = convert_to_avro(data_df, AVRO_SCHEMA)
        except RuntimeError as e:
            logger.error(f"{e}")
            avro_path = None
        return {"avro_file_path": avro_path}

    @task
    def process_data(data: Dict[str, Any]) -> str:
        """
        Read tmp binary file and apply a schema on it to validate data.
        Sum the values by key and then return the key with the 3rd largest result for the given date.

        return: 3rd largest result
        """

        avro_path = data["avro_file_path"]
        logger.info(f"Reading AVRO file from path: {avro_path}")

        try:
            data_df = read_avro_file(avro_path)
        except RuntimeError as e:
            logger.error(f"Failed to read Avro file from path {avro_path}: {e}")
            data_df = None

        try:
            largest_key = get_largest_key(data_df, 3)
        except ValueError as e:
            logger.error(f"An error occurred: {e}")
            largest_key = None
        return largest_key

    start_process = EmptyOperator(task_id="start_process")
    # date_file_ds = ["2022-04-26", "2022-04-27", "2022-04-28"]
    date_file_ds = "2022-04-26"
    avro_data = source_data(ds=date_file_ds)
    result = process_data(avro_data)
    end_process = EmptyOperator(task_id="end_process")

    start_process >> avro_data >> result >> end_process


dag = task_2_stream()
