from unittest import mock
import pandas as pd
import pytest
import duckdb

from src.open_house_dashboard import OpenHouseDashboard


@pytest.fixture(autouse=True)
def mock_read_parquet(monkeypatch):
    def mock_read_parquet(*args, **kwargs):
        # Create a mock DataFrame for testing - data does not matter
        data = [
            {'OpenHouseDate': '2023-01-01', 'Zipcode': '12345'},
            {'OpenHouseDate': '2023-01-02', 'Zipcode': '23456'},
            {'OpenHouseDate': '2023-01-03', 'Zipcode': '34567'},
        ]
        return pd.DataFrame(data)

    monkeypatch.setattr(pd, 'read_parquet', mock_read_parquet)


@pytest.fixture
def dashboard(mock_read_parquet):
    # Create a dashboard instance
    test_data_path = 'test_data.parquet'
    dashboard = OpenHouseDashboard(test_data_path)
    yield dashboard
    dashboard.close()


@mock.patch.object(duckdb.DuckDBPyConnection, 'execute')
def test_most_open_houses_week(mock_execute, dashboard):
    # Mocked SQL query results
    mocked_most_open_houses_week = pd.DataFrame({
        'Week': [13],
        'StartOfWeek': ['2023-03-27'],
        'EndOfWeek': ['2023-04-02'],
        'OpenHouseCount': [528]
    })

    # Mock the SQL query execution and return the mocked result
    mock_execute.return_value.df.return_value = mocked_most_open_houses_week

    # Call the method under test
    result_df = dashboard._query(dashboard.get_week_most_open_houses_query())

    # Assert the result
    assert result_df.equals(mocked_most_open_houses_week)


@mock.patch.object(duckdb.DuckDBPyConnection, 'execute')
def test_top_5_zip_codes(mock_execute, dashboard):
    # Mocked SQL query results
    mocked_top_5_zip_codes = pd.DataFrame({
        'Zipcode': ['12345', '67890', '54321', '98765', '13579'],
        'OpenHouseCount': [100, 90, 80, 70, 60]
    })

    # Mock the SQL query execution and return the mocked result
    mock_execute.return_value.df.return_value = mocked_top_5_zip_codes

    # Call the method under test
    result_df = dashboard._query(dashboard.get_top_zip_codes_query())

    # Assert the result
    assert result_df.equals(mocked_top_5_zip_codes)


@mock.patch.object(duckdb.DuckDBPyConnection, 'execute')
def test_daily_cumulative_total(mock_execute, dashboard):
    # Mocked SQL query results
    mocked_daily_cumulative_total = pd.DataFrame({
        'OpenHouseDate': ['2023-03-01', '2023-03-02', '2023-03-03'],
        'daily_cumulative_total': [10, 20, 30]
    })

    # Mock the SQL query execution and return the mocked result
    mock_execute.return_value.df.return_value = mocked_daily_cumulative_total

    # Call the method under test
    result_df = dashboard._query(dashboard.get_daily_cumulative_total_query())

    # Assert the result
    assert result_df.equals(mocked_daily_cumulative_total)


@mock.patch.object(duckdb.DuckDBPyConnection, 'execute')
@mock.patch('src.open_house_dashboard.st')
def test_display_dashboard(mock_st, mock_execute, dashboard):
    # Call the method under test
    dashboard.display_dashboard()

    # Assertions for Streamlit function calls
    mock_st.title.assert_called_once_with('Open House Dashboard')
    mock_st.subheader.assert_called_with('Daily Cumulative Total of Open Houses Over Time')
    mock_st.write.assert_called()
    mock_st.line_chart.assert_called()

    # Assertions for queries in Duckdb invoked
    assert dashboard.get_week_most_open_houses_query() in [call[0][0] for call in mock_execute.call_args_list]
    assert dashboard.get_top_zip_codes_query() in [call[0][0] for call in mock_execute.call_args_list]
    assert dashboard.get_daily_cumulative_total_query() in [call[0][0] for call in mock_execute.call_args_list]

    # Clean up
    dashboard.close()


if __name__ == '__main__':
    # Run the tests
    pytest.main()
