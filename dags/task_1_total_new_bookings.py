import logging
import os
from typing import Dict

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlmodel import SQLModel

from common.task1_funcs import (
    calculate_total_bookings_by_country,
    ingest_bronze_to_silver_func,
    ingest_silver_to_gold_func,
    ingest_to_bronze_func,
)
from config import DATA_PATH
from hooks.sqlite_hook_custom import CustomSqliteHook
from models.models import (
    BookingBronze,
    BookingGold,
    BookingSilver,
    PassengerBronze,
    PassengerGold,
    PassengerSilver,
)

logger = logging.getLogger("airflow.task")


SqliteHook = CustomSqliteHook()


@dag(
    schedule_interval="@daily",
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_1_total_new_bookings() -> DAG:
    """
    DAG processing data from csv files. Storing, processing, and reading the contents using SQlite
    """

    @task
    def ingest_passenger_bronze(csv_path: str, table_model: SQLModel):
        logger.info(f"Ingesting data from {csv_path} to {table_model.__tablename__}")
        try:
            ingest_to_bronze_func(csv_path, table_model)
        except Exception as e:
            logger.error(f"Failed to ingest data to bronze: {e}")
            raise

    @task
    def ingest_booking_bronze(csv_path: str, table_model: SQLModel):
        logger.info(f"Ingesting data from {csv_path} to {table_model.__tablename__}")
        ingest_to_bronze_func(csv_path, table_model)

    @task
    def ingest_passenger_bronze_to_silver(
        bronze_table: SQLModel, silver_table: SQLModel
    ):
        """Ingest passenger data from bronze to silver table"""
        logger.info("Ingesting passenger data from bronze to silver table")
        ingest_bronze_to_silver_func(bronze_table, silver_table)

    @task
    def ingest_booking_bronze_to_silver(bronze_table: SQLModel, silver_table: SQLModel):
        """Ingest booking data from bronze to silver table"""
        logger.info("Ingesting booking data from bronze to silver table")
        ingest_bronze_to_silver_func(bronze_table, silver_table)

    @task
    def ingest_passenger_silver_to_gold(silver_table: SQLModel, gold_table: SQLModel):
        """Ingest passenger data from silver to gold table"""
        logger.info("Ingesting passenger data from silver to gold table")
        ingest_silver_to_gold_func(silver_table, gold_table)

    @task
    def ingest_booking_silver_to_gold(silver_table: SQLModel, gold_table: SQLModel):
        """Ingest booking data from silver to gold table"""
        logger.info("Ingesting booking data from silver to gold table")
        ingest_silver_to_gold_func(silver_table, gold_table)

    @task
    def calculate_total_new_bookings_by_country() -> Dict[str, any]:
        """
        Calculate the total number of bookings for new passengers based on their country of origin.
            * New passengers are defined as who registered >= `2021-01-01 00:00:00`.
            * If `country_code` is empty, passengers should be categorized as `OTHER`.

        Save the result to total_new_booking table

        :return: Dict of saved table metadata
        """

        df_passengers = SqliteHook.fetch_dataframe(PassengerGold)
        df_bookings = SqliteHook.fetch_dataframe(BookingGold)
        df_passengers["date_registered"] = pd.to_datetime(
            df_passengers["date_registered"]
        )
        new_passengers = df_passengers[df_passengers["date_registered"] >= "2021-01-01"]
        merged_df = pd.merge(
            new_passengers,
            df_bookings,
            left_on="id",
            right_on="id_passenger",
            how="left",
        )
        try:
            total_bookings = calculate_total_bookings_by_country(merged_df)
        except RuntimeError as e:
            logger.error(f"Failed to calculate total bookings by country: {e}")
            raise

        SqliteHook.insert_dataframe_to_sql(total_bookings, "total_new_booking")

        table_metadata = {
            "table_name": "total_new_booking",
            "column_count": len(total_bookings.columns),
            "row_count": len(total_bookings),
            "columns": list(total_bookings.columns),
            "dtypes": total_bookings.dtypes.apply(lambda x: x.name).to_dict(),
        }

        return table_metadata

    @task(task_id="print_data")
    def print_data(table: Dict[str, any]) -> None:
        """
        Read table data from sqlite based on input dict and print to console.

        :param table: Dict of table metadata
        :return: None
        """
        with SqliteHook.get_conn() as connection:
            query = f"SELECT * FROM {table['table_name']} ORDER BY total_bookings DESC"
            df = pd.read_sql_query(query, connection)
        logger.info(f"\nData Frame: \n{df.to_markdown()}")

    start_process = EmptyOperator(task_id="start_process")

    path_data_passenger = os.path.join(DATA_PATH, "passenger.csv")
    path_data_booking = os.path.join(DATA_PATH, "booking.csv")
    task_ingest_passenger_bronze = ingest_passenger_bronze(
        path_data_passenger, PassengerBronze
    )
    task_ingest_booking_bronze = ingest_booking_bronze(path_data_booking, BookingBronze)

    task_ingest_passenger_bronze_to_silver = ingest_passenger_bronze_to_silver(
        PassengerBronze, PassengerSilver
    )
    task_ingest_booking_bronze_to_silver = ingest_booking_bronze_to_silver(
        BookingBronze, BookingSilver
    )

    task_ingest_passenger_silver_to_gold = ingest_passenger_silver_to_gold(
        PassengerSilver, PassengerGold
    )
    task_ingest_booking_silver_to_gold = ingest_booking_silver_to_gold(
        BookingSilver, BookingGold
    )

    task_calculate = calculate_total_new_bookings_by_country()
    task_print = print_data(task_calculate)

    end_process = EmptyOperator(task_id="end_process")

    (
        start_process
        >> task_ingest_passenger_bronze
        >> task_ingest_passenger_bronze_to_silver
        >> task_ingest_passenger_silver_to_gold
    )
    (
        start_process
        >> task_ingest_booking_bronze
        >> task_ingest_booking_bronze_to_silver
        >> task_ingest_booking_silver_to_gold
    )

    (
        [task_ingest_passenger_silver_to_gold, task_ingest_booking_silver_to_gold]
        >> task_calculate
        >> task_print
        >> end_process
    )


dag = task_1_total_new_bookings()
