import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlmodel import SQLModel

from common.task1_funcs import (
    SqliteHook,
    calculate_total_bookings_by_country,
    fetch_data_from_table,
    ingest_bronze_to_silver_func,
    ingest_silver_to_gold_func,
    ingest_to_bronze_func,
)
from models.models import (
    BookingBronze,
    BookingGold,
    BookingSilver,
    PassengerBronze,
    PassengerGold,
    PassengerSilver,
)


@pytest.fixture
def sample_dataframes():
    return {
        "passenger": pd.DataFrame(
            {
                "id": ["1", "2"],
                "date_registered": ["2000-01-06", "2012-02-12"],
                "country_code": ["BR", "DE"],
            }
        ),
        "booking": pd.DataFrame(
            {
                "id": ["1", "2"],
                "id_passenger": ["1", "2"],
                "date_created": ["2023-01-01", "2023-01-02"],
                "date_close": ["2023-01-10", "2023-01-11"],
            }
        ),
    }


@pytest.fixture
def mock_sqlite_hook(mocker):
    mocker.patch("common.task1_funcs.SqliteHook", autospec=True)
    return SqliteHook


@pytest.mark.parametrize(
    "data_type, bronze_model, sample_data",
    [
        ("passenger", PassengerBronze, "passenger"),
        ("booking", BookingBronze, "booking"),
    ],
)
def test_ingest_to_bronze_func(
    mocker, sample_dataframes, data_type, bronze_model, sample_data, tmp_path
):
    df = sample_dataframes[sample_data]
    csv_path = os.path.join(tmp_path, f"sample_{data_type}.csv")
    df.to_csv(csv_path, index=False)

    mock_insert_rows = mocker.patch.object(SqliteHook, "insert_rows", autospec=True)
    ingest_to_bronze_func(csv_path, bronze_model)
    mock_insert_rows.assert_called_once()
    assert mock_insert_rows.call_args[1]["table"] == bronze_model.__tablename__
    assert mock_insert_rows.call_args[1]["target_fields"] == df.columns.tolist()


@pytest.mark.parametrize(
    "bronze_model, silver_model, sample_data",
    [
        (PassengerBronze, PassengerSilver, "passenger"),
        (BookingBronze, BookingSilver, "booking"),
    ],
)
def test_ingest_bronze_to_silver_func(
    mocker, sample_dataframes, bronze_model, silver_model, sample_data
):
    df = sample_dataframes[sample_data]
    mocker.patch.object(SqliteHook, "fetch_dataframe", return_value=df)
    mock_insert_rows = mocker.patch.object(SqliteHook, "insert_rows", autospec=True)
    mocker.patch(
        "common.task1_funcs.convert_columns_to_datetime",
        side_effect=lambda df, cols: df,
    )
    mocker.patch(
        "common.task1_funcs.treat_country_code_data", side_effect=lambda df, col: df
    )
    mocker.patch("common.task1_funcs.treat_general_data", side_effect=lambda df: df)

    ingest_bronze_to_silver_func(bronze_model, silver_model)

    mock_insert_rows.assert_called_once()
    assert mock_insert_rows.call_args[1]["table"] == silver_model.__tablename__
    assert mock_insert_rows.call_args[1]["target_fields"] == df.columns.tolist()
    assert mock_insert_rows.call_args[1]["rows"] == df.values.tolist()


@pytest.mark.parametrize(
    "silver_model, gold_model, sample_data",
    [
        (PassengerSilver, PassengerGold, "passenger"),
        (BookingSilver, BookingGold, "booking"),
    ],
)
def test_ingest_silver_to_gold_func(
    mocker, sample_dataframes, silver_model, gold_model, sample_data
):
    df = sample_dataframes[sample_data]
    mocker.patch.object(SqliteHook, "fetch_dataframe", return_value=df)
    mock_insert_rows = mocker.patch.object(SqliteHook, "insert_rows", autospec=True)
    mocker.patch("common.task1_funcs.treat_general_data", side_effect=lambda df: df)

    ingest_silver_to_gold_func(silver_model, gold_model)

    mock_insert_rows.assert_called_once()
    assert mock_insert_rows.call_args[1]["table"] == gold_model.__tablename__
    assert mock_insert_rows.call_args[1]["target_fields"] == df.columns.tolist()
    assert mock_insert_rows.call_args[1]["rows"] == df.values.tolist()


def test_fetch_data_from_table_success(mocker):
    connection_mock = mocker.MagicMock()
    mocker.patch("common.task1_funcs.SqliteHook.get_conn", return_value=connection_mock)
    mocker.patch(
        "pandas.read_sql_query",
        return_value=pd.DataFrame({"column1": [1, 2], "column2": [3, 4]}),
    )

    table = {"table_name": "test_table"}
    result = fetch_data_from_table(table)

    assert not result.empty
    assert list(result.columns) == ["column1", "column2"]


def test_calculate_total_bookings_by_country():
    data = {
        "country_code": ["DE", "DE", "BR", "BR", "BR", "US"],
        "booking_id": [1, 2, 3, 4, 5, 6],
    }
    merged_df = pd.DataFrame(data)

    expected_data = {"country_code": ["BR", "DE", "US"], "total_bookings": [3, 2, 1]}
    expected_df = pd.DataFrame(expected_data)

    result_df = calculate_total_bookings_by_country(merged_df)

    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_calculate_total_bookings_by_country_exception():
    data = {"booking_id": [1, 2, 3, 4, 5, 6]}
    merged_df = pd.DataFrame(data)

    with pytest.raises(
        RuntimeError, match="Failed to calculate total bookings by country"
    ):
        calculate_total_bookings_by_country(merged_df)
