import json
import os
import tempfile
from typing import Dict, Union

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from common.data_treatment import treat_general_data


def read_csv_file_to_df(file_path: Union[str, dict]) -> pd.DataFrame:
    """Read CSV file and return DataFrame."""
    if isinstance(file_path, str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist.")
        return pd.read_csv(file_path)
    else:
        if isinstance(file_path, dict):
            df_list = []
            file_path = file_path
            for date, path in file_path.items():
                if not os.path.exists(path):
                    raise FileNotFoundError(f"File {path} does not exist.")
                df = pd.read_csv(path, header=None, names=["key", "value"])
                df["date"] = date
                df_list.append(df)
            return pd.concat(df_list)


def convert_to_avro(df: pd.DataFrame, avro_schema: json) -> str:
    """Convert DataFrame to Avro file."""
    try:
        schema_parse = schema.parse(json.dumps(avro_schema))
        records = df.to_dict(orient="records")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".avro") as tmp_file:
            with open(tmp_file.name, "wb") as avro_file:
                writer = DataFileWriter(avro_file, DatumWriter(), schema_parse)
                for record in records:
                    writer.append(record)
                writer.close()
            avro_path = tmp_file.name

        return avro_path
    except Exception as e:
        raise RuntimeError("Failed to convert DataFrame to Avro: {e}")


def read_avro_file(avro_path: str) -> pd.DataFrame:
    """Read Avro file and return as DataFrame."""
    try:
        with open(avro_path, "rb") as avro_file:
            reader = DataFileReader(avro_file, DatumReader())
            records = [record for record in reader]
            reader.close()
        df = pd.DataFrame(records)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to read Avro file at {avro_path}: {e}")


def get_largest_key(df: pd.DataFrame, n: int) -> Union[pd.DataFrame, None]:
    """
    Get the n largest key from the DataFrame
    """
    if len(df) == 0:
        raise ValueError("No data available")
    df = treat_general_data(
        df, drop_duplicates=False, add_zero_values=True, value_column="value"
    )
    grouped_df = df.groupby("key").sum().reset_index()
    sorted_df = grouped_df.sort_values(by="value", ascending=False)

    if len(sorted_df) < n:
        raise ValueError(f"Not enough data to find the {n}th largest result")
    else:
        return sorted_df.iloc[n - 1]["key"]
