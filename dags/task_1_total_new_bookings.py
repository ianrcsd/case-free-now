from typing import Dict

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
from sqlmodel import SQLModel
from models.models import BookingBronze, BookingSilver, BookingGold, PassengerBronze, PassengerSilver, PassengerGold
from common.ingestions import convert_to_data_frame, ingest_to_bronze_func, ingest_bronze_to_silver_func, ingest_silver_to_gold_func


@dag(
    schedule_interval="@daily",
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    description="Reading data from a CSV file, processing the data and storing it",
)
def task_1_total_new_bookings() -> DAG:
    """
    DAG processing data from csv files. Storing, processing, and reading the contents using SQlite
    """

    @task
    def ingest_passenger_bronze(csv_path: str, table_model: SQLModel):
        ingest_to_bronze_func(csv_path, table_model)

    @task
    def ingest_booking_bronze(csv_path: str, table_model: SQLModel):
        ingest_to_bronze_func(csv_path, table_model)

    @task
    def ingest_passenger_bronze_to_silver(bronze_table: SQLModel, silver_table: SQLModel):
        ingest_bronze_to_silver_func(bronze_table, silver_table)

    @task
    def ingest_booking_bronze_to_silver(bronze_table: SQLModel, silver_table: SQLModel):
        ingest_bronze_to_silver_func(bronze_table, silver_table)

    @task
    def ingest_passenger_silver_to_gold(silver_table: SQLModel, gold_table: SQLModel):
        ingest_silver_to_gold_func(silver_table, gold_table)

    @task
    def ingest_booking_silver_to_gold(silver_table: SQLModel, gold_table: SQLModel):
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

        sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_conn')
    
        with sqlite_hook.get_conn() as connection:
            df_passengers = convert_to_data_frame(connection, PassengerGold)
            df_bookings = convert_to_data_frame(connection, BookingGold)

            df_passengers['date_registered'] = pd.to_datetime(df_passengers['date_registered'])
            new_passengers = df_passengers[df_passengers['date_registered'] >= '2021-01-01']

            merged_df = pd.merge(new_passengers, df_bookings, left_on='id',right_on='id_passenger', how='left')

            total_bookings = merged_df.groupby('country_code').size().reset_index(name='total_bookings')

            total_bookings.to_csv('total_new_booking.csv', index=False)

            total_bookings.to_sql('total_new_booking', connection, if_exists='replace', index=False)

        return {"table_name": "total_new_booking", "row_count": len(total_bookings)}

    @task(task_id="print_data")
    def print_data(table: Dict[str, any]) -> None:
        """
        Read table data from sqlite based on input dict and print to console.

        :param table: Dict of table metadata
        :returns: None
        """
        sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_conn')
        connection = sqlite_hook.get_conn()
        query = f"SELECT * FROM {table['table_name']}"
        df = pd.read_sql_query(query, connection)
        connection.close()
        print(df)

    path_data_passenger = "dags/data/passenger.csv"
    path_data_booking = "dags/data/booking.csv"
    task_ingest_passenger_bronze = ingest_passenger_bronze(path_data_passenger, PassengerBronze)
    task_ingest_booking_bronze = ingest_booking_bronze(path_data_booking, BookingBronze)

    task_ingest_passenger_bronze_to_silver = ingest_passenger_bronze_to_silver(PassengerBronze, PassengerSilver)
    task_ingest_booking_bronze_to_silver = ingest_booking_bronze_to_silver(BookingBronze, BookingSilver)

    task_ingest_passenger_silver_to_gold = ingest_passenger_silver_to_gold(PassengerSilver, PassengerGold)
    task_ingest_booking_silver_to_gold = ingest_booking_silver_to_gold(BookingSilver, BookingGold)

    task_calculate = calculate_total_new_bookings_by_country()
    task_print = print_data(task_calculate)


    task_ingest_passenger_bronze >> task_ingest_passenger_bronze_to_silver >> task_ingest_passenger_silver_to_gold
    task_ingest_booking_bronze >> task_ingest_booking_bronze_to_silver >> task_ingest_booking_silver_to_gold

    [task_ingest_passenger_silver_to_gold, task_ingest_booking_silver_to_gold] >> task_calculate >> task_print


dag = task_1_total_new_bookings()
