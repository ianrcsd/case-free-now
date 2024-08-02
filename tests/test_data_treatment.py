import pandas as pd

from common.data_treatment import (
    convert_columns_to_datetime,
    treat_country_code_data,
    treat_general_data,
)


def test_treat_general_data():
    data = {
        "col1": [" A ", " B ", None, "C"],
        "col2": [None, None, None, None],
        "col3": [" ", "B", " C ", "C"],
        "col4": [1, 2, 1, 1],
    }
    df = treat_general_data(pd.DataFrame(data))
    print(df)
    expected_data = {
        "col1": ["A", "B", None, "C"],
        "col3": [None, "B", "C", "C"],
        "col4": [1, 2, 1, 1],
    }

    expected_df = pd.DataFrame(expected_data)
    print(expected_df)
    pd.testing.assert_frame_equal(df, expected_df)


def test_convert_columns_to_datetime():
    data = {
        "date1": ["2024-08-01 12:00:00", "invalid_date", "2024-08-02 13:00:00"],
        "date2": ["2024-08-03 14:00:00", "2024-08-04 15:00:00", "not_a_date"],
    }
    df = pd.DataFrame(data)
    df = convert_columns_to_datetime(df, ["date1", "date2"])

    expected_data = {
        "date1": [
            pd.Timestamp("2024-08-01 12:00:00"),
            pd.NaT,
            pd.Timestamp("2024-08-02 13:00:00"),
        ],
        "date2": [
            pd.Timestamp("2024-08-03 14:00:00"),
            pd.Timestamp("2024-08-04 15:00:00"),
            pd.NaT,
        ],
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df, expected_df)


def test_treat_country_code_data():
    data = {"country_code": ["us", "gb", "xyz", "IN", None, "ca"]}
    df = pd.DataFrame(data)
    df = treat_country_code_data(df, "country_code")

    expected_data = {"country_code": ["US", "GB", "OTHER", "IN", "OTHER", "CA"]}
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df, expected_df)
