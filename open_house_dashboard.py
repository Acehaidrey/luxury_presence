import duckdb
import pandas as pd
import streamlit as st


class OpenHouseDashboard:
    """
    A class to create an Open House Dashboard using Streamlit and DuckDB.

    Attributes:
        data_path (str): The path to the cleaned open house data in Parquet format.
    """

    def __init__(self, data_path):
        """
        Initialize the OpenHouseDashboard with the path to the cleaned open house data.

        Args:
            data_path (str): The path to the cleaned open house data in Parquet format.
        """
        self.data_path = data_path
        self.df = pd.read_parquet(data_path)
        self.con = duckdb.connect(database=':memory:', read_only=False)
        self.con.register("openhouses", self.df)

    def _query(self, sql):
        """
        Execute an SQL query and return the result as a pandas DataFrame.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            pandas.DataFrame: The result of the SQL query.
        """
        return self.con.execute(sql).df()

    def display_dashboard(self):
        """
        Display the Open House Dashboard using Streamlit components.
        """
        st.title('Open House Dashboard')

        # Week with the most open houses
        most_open_houses_week = '''
        Fill in the sql query
        '''
        most_open_houses_week_df = self._query(most_open_houses_week)
        # print(most_open_houses_week_df)
        st.subheader('Week with the Most Open Houses')
        st.write(most_open_houses_week_df)

        # Top-5 zip codes with the most open houses
        top_5_zip_codes = '''
        Fill in the sql query
        '''
        top_5_zip_codes_df = self._query(top_5_zip_codes)
        # print(top_5_zip_codes_df)
        st.subheader('Top-5 Zip Codes with the Most Open Houses')
        st.write(top_5_zip_codes_df)

        # Daily cumulative total of open houses over time
        daily_cumulative_total = '''
        Fill in the sql query
        '''
        daily_cumulative_total_df = self._query(daily_cumulative_total)
        # print(daily_cumulative_total_df)
        st.subheader('Daily Cumulative Total of Open Houses Over Time')
        st.line_chart(daily_cumulative_total_df, y="daily_cumulative_total", x="OpenHouseDate")

    def close(self):
        """
        Close the DuckDB connection.
        """
        self.con.close()


# Example usage:
dashboard = OpenHouseDashboard('data/processed_openhouses.parquet')
dashboard.display_dashboard()
dashboard.close()
