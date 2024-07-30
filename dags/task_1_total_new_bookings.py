from typing import Dict

import pendulum
from airflow import DAG
from airflow.decorators import dag, task


@dag(
    schedule="@daily",
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_1_total_new_bookings() -> DAG:
    """
    DAG processing data from csv files. Storing, processing, and reading the contents using SQlite
    """

    @task
    def ingest_csv_as_table(csv_path: str, table_name: str) -> None:
        """
        Read csv dataset and inserts all rows into a table.

        """
        raise NotImplementedError

    @task
    def calculate_total_new_bookings_by_country() -> Dict[str, any]:
        """
        Calculate the total number of bookings for new passengers based on their country of origin.
            * New passengers are defined as who registered >= `2021-01-01 00:00:00`.
            * If `country_code` is empty, passengers should be categorized as `OTHER`.

        Save the result to total_new_booking table

        :return: Dict of saved table metadata
        """
        raise NotImplementedError

    @task
    def print_data(table: Dict[str, any]) -> None:
        """
        Read table data from sqlite based on input dict and print to console.

        :param table: Dict of table metadata
        :returns: None
        """
        raise NotImplementedError

    task_ingest_passenger = ingest_csv_as_table("passenger_csv_path", "passenger")
    task_ingest_booking = ingest_csv_as_table("booking_csv_path", "booking")
    task_calculate = calculate_total_new_bookings_by_country()
    task_print = print_data(task_calculate)

    [task_ingest_passenger, task_ingest_booking] >> task_calculate >> task_print


dag = task_1_total_new_bookings()
