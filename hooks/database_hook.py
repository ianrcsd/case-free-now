from abc import ABC, abstractmethod
from typing import Type

import pandas as pd
from sqlmodel import SQLModel


class DatabaseHook(ABC):

    @abstractmethod
    def fetch_dataframe(self, table: Type[SQLModel]) -> pd.DataFrame:
        pass

    @abstractmethod
    def insert_dataframe(self, table: Type[SQLModel], df: pd.DataFrame):
        pass

    @abstractmethod
    def insert_dataframe_to_sql(self, df: pd.DataFrame, table_name: str) -> None:
        pass
