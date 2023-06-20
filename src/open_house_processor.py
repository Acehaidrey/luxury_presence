import json
import os

import pandas as pd


class OpenHouseProcessor:
    """
    A class to process open house data from a JSON file, clean and process the data,
    and write the output to a Parquet file.

    Attributes:
        input_path (str): The path to the input JSON file containing open house data.
        output_path (str): The path to the output Parquet file to store cleaned data.
    """

    def __init__(self, input_path, output_path):
        """
        Initialize the OpenHouseProcessor with input and output paths.

        Args:
            input_path (str): The path to the input JSON file containing open house data.
            output_path (str): The path to the output Parquet file to store cleaned data.
        """
        self.input_path = os.path.abspath(input_path)
        self.output_path = os.path.abspath(output_path)

    def read_data(self):
        """
        Read the raw open house data from the input JSON file.

        Note, if the input is too large to handle with a single read, it is recommended to use
        techniques like sharding or multiprocessing to read the data in parallel and combine the results.
        Alternatively, you can consider using distributed computing frameworks like MapReduce to process
        the large dataset efficiently.

        Returns:
            list: A list of dictionaries containing the open house data.
        """
        with open(self.input_path, 'r') as file:
            data = json.load(file)
        return data

    def process_data(self, data):
        """
        Process the raw open house data by converting it to a pandas DataFrame,
        and include only valid records in the output.

        Args:
            data (list): A list of dictionaries containing the raw open house data.

        Returns:
            pandas.DataFrame: A cleaned DataFrame containing valid open house data records.
        """
        # Convert data to DataFrame
        df = pd.DataFrame(data)

        # Drop records with null OpenHouseKey and invalid timestamps (convert timestamp then drop null types)
        df['OpenHouseStartTime'] = pd.to_datetime(df['OpenHouseStartTime'], utc=True, errors='coerce')
        df['OpenHouseEndTime'] = pd.to_datetime(df['OpenHouseEndTime'], utc=True, errors='coerce')
        df.dropna(subset=['OpenHouseKey', 'OpenHouseStartTime', 'OpenHouseEndTime'], inplace=True)

        # Keep only the latest record for each OpenHouseKey
        df['DateModified'] = pd.to_datetime(df['DateModified'], utc=True)
        df.sort_values('DateModified', ascending=False, inplace=True)
        df.drop_duplicates('OpenHouseKey', keep='first', inplace=True)

        return df

    def write_data(self, df):
        """
        Write the cleaned open house data to a Parquet file.

        Note, we do not create an index on the output parquet file to keep
        it smaller and more performant but if downstream processes will be
        querying on unique keys like OpenHouseKey, we should consider setting.

        Args:
            df (pandas.DataFrame): The cleaned DataFrame containing open house data.
        """
        df.to_parquet(self.output_path, index=False)
        print(f'Created file `{self.output_path}` with the cleaned results.')

    def run(self):
        """
        Run the OpenHouseProcessor by reading the data, processing it, and writing the cleaned data to a Parquet file.
        """
        data = self.read_data()
        cleaned_data = self.process_data(data)
        print(
            f'\nInput file `{self.input_path}` produced {len(data)} raw records.\n'
            f'Processing produced {len(cleaned_data)} cleaned records.\n'
            f'Output file written in parquet to `{self.output_path}`.'
        )
        self.write_data(cleaned_data)


if __name__ == '__main__':
    # example use case
    input_path = '../data/openhouses.json'
    output_path = '../data/output/processed_openhouses.parquet'
    processor = OpenHouseProcessor(input_path, output_path)
    processor.run()
