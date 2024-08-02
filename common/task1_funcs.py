from typing import Dict

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlmodel import SQLModel

from common.data_treatment import (
    convert_columns_to_datetime,
    treat_country_code_data,
    treat_general_data,
)


def convert_to_table_to_data_frame(connection, table: SQLModel) -> pd.DataFrame:
    """
    Convert SQLModel rows to pandas DataFrame
    """
    query = f"SELECT * FROM {table.__tablename__}"
    return pd.read_sql_query(query, connection)


def ingest_to_bronze_func(csv_path: str, table_name: SQLModel) -> None:
    """
    Read csv dataset and inserts all rows into a table.
    """
    df = pd.read_csv(csv_path, sep=",")
    df = df.where(pd.notnull(df), None)
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_conn")
    sqlite_hook.insert_rows(
        table=table_name.__tablename__,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
    )


def ingest_bronze_to_silver_func(
    bronze_table: SQLModel, silver_table: SQLModel
) -> None:
    """
    Read data from bronze table, process it and insert into silver table.
    """
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_conn")
    connection = sqlite_hook.get_conn()
    df = convert_to_table_to_data_frame(connection, bronze_table)

    data_columns = [col for col in df.columns if "data" in col.lower()]
    df = convert_columns_to_datetime(df, data_columns) if data_columns else df
    df = (
        treat_country_code_data(df, "country_code")
        if "country_code" in df.columns
        else df
    )

    df = treat_general_data(df)
    sqlite_hook.insert_rows(
        table=silver_table.__tablename__,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
    )
    connection.close()


def ingest_silver_to_gold_func(silver_table: SQLModel, gold_table: SQLModel) -> None:
    """
    Read data from silver table, process it and insert into gold table.
    """
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_conn")
    connection = sqlite_hook.get_conn()
    df = convert_to_table_to_data_frame(connection, silver_table)

    df = treat_general_data(df)

    sqlite_hook.insert_rows(
        table=gold_table.__tablename__,
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
    )
    connection.close()
