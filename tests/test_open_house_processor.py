import os
import unittest
import json
from unittest.mock import patch, mock_open
import pandas as pd
from src.open_house_processor import OpenHouseProcessor


class TestOpenHouseProcessor(unittest.TestCase):

    def setUp(self):
        # Set up test input and output paths (fake paths)
        self.input_path = 'test/openhouses.json'
        self.output_path = 'test/test_processed_openhouses.parquet'

    def test_process_data_multiple_records_openhousekey(self):
        # Test case: Multiple rows with the same OpenHouseKey
        data = [
            {
                'OpenHouseMethod': 'In-person',
                'OpenHouseEndTime': '2023-06-18T10:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-18T08:00:00Z',
                'OpenHouseDate': '2023-06-18',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-18T12:00:00Z'
            },
            {
                'OpenHouseMethod': 'Virtual',
                'OpenHouseEndTime': '2023-06-19T11:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-19T09:00:00Z',
                'OpenHouseDate': '2023-06-19',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-19T13:00:00Z'
            }
        ]

        # Create an instance of OpenHouseProcessor
        processor = OpenHouseProcessor(self.input_path, self.output_path)

        # Call the process_data method
        cleaned_data = processor.process_data(data)

        # Perform assertions
        self.assertIsInstance(cleaned_data, pd.DataFrame)
        self.assertEqual(len(cleaned_data), 1)  # Only one record should remain
        self.assertEqual(cleaned_data.iloc[0]['OpenHouseMethod'], 'Virtual')  # The latest record should be kept

    def test_process_data_invalid_timestamp(self):
        # Test case: Invalid timestamps (missing OpenHouseStartTime and OpenHouseEndTime)
        data = [
            {
                'OpenHouseMethod': 'In-person',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseDate': '2023-06-18',
                'State': 'CA',
                'Zipcode': '12345',
                'DateModified': '2023-06-18T12:00:00Z'
            },
            {
                'OpenHouseMethod': 'Virtual',
                'OpenHouseEndTime': '2023-06-19T11:00:00Z',
                'ListingKey': '67890',
                'OpenHouseKey': '54321',
                'OpenHouseStartTime': '2023-06-19T09:00:00Z',
                'OpenHouseDate': '2023-06-19',
                'State': 'CA',
                'Zipcode': '54321',
                'DateModified': '2023-06-19T13:00:00Z'
            }
        ]

        # Create an instance of OpenHouseProcessor
        processor = OpenHouseProcessor(self.input_path, self.output_path)

        # Call the process_data method
        cleaned_data = processor.process_data(data)

        # Perform assertions
        self.assertIsInstance(cleaned_data, pd.DataFrame)
        self.assertEqual(len(cleaned_data), 1)  # Only one valid record should remain
        self.assertEqual(cleaned_data.iloc[0]['OpenHouseKey'], '54321')  # Valid record should be kept

    def test_process_data_null_openhousekey(self):
        # Test case: OpenHouseKey is None
        data = [
            {
                'OpenHouseMethod': 'In-person',
                'OpenHouseEndTime': '2023-06-18T10:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-18T08:00:00Z',
                'OpenHouseDate': '2023-06-18',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-18T12:00:00Z'
            },
            {
                'OpenHouseMethod': 'Virtual',
                'OpenHouseEndTime': '2023-06-19T11:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': None,  # OpenHouseKey is None
                'OpenHouseStartTime': '2023-06-19T09:00:00Z',
                'OpenHouseDate': '2023-06-19',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-19T13:00:00Z'
            }
        ]

        # Create an instance of OpenHouseProcessor
        processor = OpenHouseProcessor(self.input_path, self.output_path)

        # Call the process_data method
        cleaned_data = processor.process_data(data)

        # Perform assertions
        self.assertIsInstance(cleaned_data, pd.DataFrame)
        self.assertEqual(len(cleaned_data), 1)  # Only one record should remain
        self.assertEqual(cleaned_data.iloc[0]['OpenHouseMethod'], 'In-person')  # The valid record should be kept

    def test_read_data(self):
        # Test case: Valid JSON data
        expected_data = [
            {
                'OpenHouseMethod': 'In-person',
                'OpenHouseEndTime': '2023-06-18T10:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-18T08:00:00Z',
                'OpenHouseDate': '2023-06-18',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-18T12:00:00Z'
            },
            {
                'OpenHouseMethod': 'Virtual',
                'OpenHouseEndTime': '2023-06-19T11:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-19T09:00:00Z',
                'OpenHouseDate': '2023-06-19',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-19T13:00:00Z'
            }
        ]

        # Mock the open function to read JSON data
        with patch('builtins.open', mock_open(read_data=json.dumps(expected_data))):
            processor = OpenHouseProcessor(self.input_path, self.output_path)
            actual_data = processor.read_data()

        # Perform assertions
        self.assertEqual(actual_data, expected_data)

    def test_write_data(self):
        # Test case: Valid DataFrame
        data = {
            'OpenHouseMethod': ['In-person', 'Virtual'],
            'OpenHouseEndTime': ['2023-06-18T10:00:00Z', '2023-06-19T11:00:00Z'],
            'ListingKey': ['12345', '12345'],
            'OpenHouseKey': ['98765', '98765'],
            'OpenHouseStartTime': ['2023-06-18T08:00:00Z', '2023-06-19T09:00:00Z'],
            'OpenHouseDate': ['2023-06-18', '2023-06-19'],
            'State': ['CA', 'CA'],
            'Zipcode': ['92630', '92630'],
            'DateModified': ['2023-06-18T12:00:00Z', '2023-06-19T13:00:00Z']
        }
        df = pd.DataFrame(data)

        # Mock the to_parquet method to check if it is called with the correct arguments
        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            processor = OpenHouseProcessor(self.input_path, self.output_path)
            processor.write_data(df)

        # Perform assertions
        mock_to_parquet.assert_called_once_with(os.path.abspath(self.output_path), index=False)

    @patch('src.open_house_processor.OpenHouseProcessor.read_data')
    @patch('src.open_house_processor.OpenHouseProcessor.process_data')
    @patch('src.open_house_processor.OpenHouseProcessor.write_data')
    def test_run(self, mock_write_data, mock_process_data, mock_read_data):
        # Test case: Valid run
        data = [
            {
                'OpenHouseMethod': 'In-person',
                'OpenHouseEndTime': '2023-06-18T10:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-18T08:00:00Z',
                'OpenHouseDate': '2023-06-18',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-18T12:00:00Z'
            },
            {
                'OpenHouseMethod': 'Virtual',
                'OpenHouseEndTime': '2023-06-19T11:00:00Z',
                'ListingKey': '12345',
                'OpenHouseKey': '98765',
                'OpenHouseStartTime': '2023-06-19T09:00:00Z',
                'OpenHouseDate': '2023-06-19',
                'State': 'CA',
                'Zipcode': '92630',
                'DateModified': '2023-06-19T13:00:00Z'
            }
        ]
        cleaned_data = pd.DataFrame(data)

        # Set up the mock objects
        mock_read_data.return_value = data
        mock_process_data.return_value = cleaned_data

        # Create an instance of OpenHouseProcessor
        processor = OpenHouseProcessor(self.input_path, self.output_path)

        # Call the run method
        processor.run()

        # Perform assertions
        mock_read_data.assert_called_once()
        mock_process_data.assert_called_once_with(data)
        mock_write_data.assert_called_once_with(cleaned_data)

    def tearDown(self):
        # Clean up the test output file
        import os
        if os.path.exists(self.output_path):
            os.remove(self.output_path)


if __name__ == '__main__':
    unittest.main()
