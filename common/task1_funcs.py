from typing import Type

import pandas as pd
from sqlmodel import SQLModel

from common.data_treatment import (
    convert_columns_to_datetime,
    treat_country_code_data,
    treat_general_data,
)
from common.general import read_csv_file_to_df
from hooks.sqlite_hook_custom import CustomSqliteHook

SqliteHook = CustomSqliteHook()


def ingest_to_bronze_func(csv_path: str, table: Type[SQLModel]) -> None:
    """
    Read csv dataset and inserts all rows into a table.

    :param csv_path: Path to the CSV file.
    :param table: The SQLModel table class to insert data into.
    """
    try:
        df = read_csv_file_to_df(csv_path)
        SqliteHook.insert_rows(
            table=table.__tablename__,
            rows=df.values.tolist(),
            target_fields=df.columns.tolist(),
        )
    except Exception as e:
        raise Exception(f"Failed to ingest data to bronze: {e}")


def ingest_bronze_to_silver_func(
    bronze_table: Type[SQLModel], silver_table: Type[SQLModel]
) -> None:
    """
    Read data from bronze table, process it and insert into silver table.

    :param bronze_table: The SQLModel table class for the bronze table.
    :param silver_table: The SQLModel table class for the silver table.
    """
    df = SqliteHook.fetch_dataframe(bronze_table)

    data_columns = [col for col in df.columns if "data" in col.lower()]
    df = convert_columns_to_datetime(df, data_columns) if data_columns else df
    df = (
        treat_country_code_data(df, "country_code")
        if "country_code" in df.columns
        else df
    )

    df = treat_general_data(df)
    SqliteHook.insert_rows(
        table=silver_table.__tablename__,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
    )


def ingest_silver_to_gold_func(
    silver_table: Type[SQLModel], gold_table: Type[SQLModel]
) -> None:
    """
    Read data from silver table, process it and insert into gold table.

    :param silver_table: The SQLModel table class for the silver table.
    :param gold_table: The SQLModel table class for the gold table.
    """
    df = SqliteHook.fetch_dataframe(silver_table)
    df = treat_general_data(df)
    SqliteHook.insert_rows(
        table=gold_table.__tablename__,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
    )


def calculate_total_bookings_by_country(merged_df: pd.DataFrame) -> pd.DataFrame:
    """ "
    Calculate the total number of bookings by country.

    :param merged_df: A DataFrame with the merged data from the Passenger and Booking tables.
    :return: A DataFrame with the total bookings by country
    """
    try:
        total_bookings = (
            merged_df.groupby("country_code")
            .size()
            .reset_index(name="total_bookings")
            .sort_values(by="total_bookings", ascending=False)
        )
    except Exception as e:
        total_bookings = None
        raise RuntimeError(f"Failed to calculate total bookings by country: {e}")

    return total_bookings


def fetch_data_from_table(table: dict) -> pd.DataFrame:
    """
    Fetch data from a specified table in the database and return it as a DataFrame.

    :param table: A dictionary containing the table name under the key 'table_name'.
    :return: A DataFrame with the data from the table.
    """
    try:
        with SqliteHook.get_conn() as connection:
            query = f"SELECT * FROM {table['table_name']} ORDER BY total_bookings DESC"
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        raise Exception(f"An unexpected error occurred when reading table test_tabl")
