from typing import Type

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlmodel import SQLModel

from hooks.database_hook import DatabaseHook


class CustomSqliteHook(DatabaseHook, SqliteHook):
    def __init__(self, sqlite_conn_id: str = "sqlite_conn"):
        super().__init__(sqlite_conn_id=sqlite_conn_id)

    def fetch_dataframe(self, table: Type[SQLModel]) -> pd.DataFrame:
        """
        Fetch data from a table and return as pandas DataFrame.

        :param table: The SQLModel table class to fetch data from.
        :return: DataFrame containing the table data.
        """
        query = f"SELECT * FROM {table.__tablename__}"
        with self.get_conn() as connection:
            df = pd.read_sql_query(query, connection)
        return df

    def insert_dataframe(self, table: Type[SQLModel], df: pd.DataFrame) -> None:
        """
        Insert data from DataFrame into a table.

        :param table: The SQLModel table class to insert data into.
        :param df: DataFrame containing the data to be inserted.
        """
        self.insert_rows(
            table=table.__tablename__,
            rows=df.where(pd.notnull(df), None).values.tolist(),
            target_fields=df.columns.tolist(),
        )

    def insert_dataframe_to_sql(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Insert data from DataFrame into a table using pandas to_sql method.

        :param df: DataFrame containing the data to be inserted.
        :param table_name: The name of the table to insert data into.
        """
        with self.get_conn() as connection:
            df.to_sql(table_name, connection, if_exists="replace", index=False)
