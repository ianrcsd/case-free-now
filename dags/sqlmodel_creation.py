from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlmodel import SQLModel, create_engine
from models.models import BookingBronze, BookingSilver, BookingGold, PassengerBronze, PassengerSilver, PassengerGold


import pendulum
from airflow.decorators import dag, task


@dag(
    schedule="@once",
    start_date=pendulum.today('UTC'),
    catchup=False,
    description="Initialize the database",
)
def initialize_database() -> DAG:
    """
    DAG that initializes the database
    """
    @task
    def create_database() -> None:
        sqlite_url = "sqlite:///my_database.db"
        engine = create_engine(sqlite_url)
        SQLModel.metadata.create_all(engine)
    create_db = create_database()
    create_db

dag = initialize_database()