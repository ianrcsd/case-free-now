import os
from typing import Dict, Union

import pandas as pd


def read_csv_file_to_df(file_path: Union[str, dict]) -> pd.DataFrame:
    """Read CSV file and return DataFrame.

    :param file_path: Path to the CSV file or a dictionary with date and path.

    :return: DataFrame
    """
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
