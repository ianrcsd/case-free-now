from typing import Type

import pandas as pd
from sqlmodel import SQLModel

from common.data_treatment import (
    convert_columns_to_datetime,
    treat_country_code_data,
    treat_general_data,
)
from hooks.sqlite_hook_custom import CustomSqliteHook

SqliteHook = CustomSqliteHook()


def ingest_to_bronze_func(csv_path: str, table: Type[SQLModel]) -> None:
    """
    Read csv dataset and inserts all rows into a table.

    :param csv_path: Path to the CSV file.
    :param table: The SQLModel table class to insert data into.
    """
    df = pd.read_csv(csv_path, sep=",")
    df = df.where(pd.notnull(df), None)
    SqliteHook.insert_rows(
        table=table.__tablename__,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
    )


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
