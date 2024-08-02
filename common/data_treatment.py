import numpy as np
import pandas as pd
import pycountry


def treat_general_data(
    df: pd.DataFrame, drop_duplicates=True, add_zero_values=False, value_column=None
) -> pd.DataFrame:
    """
    Treat general data issues such as removing leading and trailing whitespaces,
    removing empty columns, replacing empty strings with None, and removing duplicates.
    Additionally, replace strings in the specified value column with 0.

    :paran df: DataFrame to be treated
    :param drop_duplicates: If True, drop duplicates
    :param add_zero_values: If True, replace strings with 0 in the value_column
    :param value_column: Column name to replace strings with 0
    """

    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
    df = df.replace("", None)
    df = df.dropna(axis=1, how="all")
    if drop_duplicates:
        df = df.drop_duplicates()
    if add_zero_values:
        df = df.fillna(0)
    if value_column and value_column in df.columns:
        df[value_column] = df[value_column].apply(
            lambda x: 0 if isinstance(x, str) else x
        )

    return df


def convert_columns_to_datetime(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert columns to datetime, if possible, if not, replace with NaT
    :param df: DataFrame to convert
    :param columns: List of columns to convert

    :return: DataFrame with the converted columns
    """
    for column in columns:
        df[column] = pd.to_datetime(
            df[column], format="%Y-%m-%d %H:%M:%S", errors="coerce"
        )
    return df


def treat_country_code_data(df: pd.DataFrame, country_code_column: str) -> pd.DataFrame:
    """First, convert all country codes to uppercase. Then, check if the country code is valid. If it is not, replace it with 'OTHER'.
    :param df: DataFrame to treat
    :param country_code_column: Column name with the country code

    :return: DataFrame with the treated country code column
    """

    def is_valid_country_code(code):
        try:
            code = code.upper()
            return pycountry.countries.get(alpha_2=code) is not None
        except AttributeError:
            return False

    df[country_code_column] = df[country_code_column].apply(
        lambda code: str(code).upper() if is_valid_country_code(code) else "OTHER"
    )
    return df
