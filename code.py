
import json
import boto3
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import unittest
from unittest.mock import patch, MagicMock

s3 = boto3.client('s3')
bucket_name = "brewery-api"

# Getting API data
def search_data():
    response = requests.get("https://api.openbrewerydb.org/breweries")
    if response.status_code == 200:
        return response.content
    else:
        print(f"Request Error: {response.status_code}")
        return None

# Saving dataFrame Parquet - S3
def save_parquet_s3(df, path):
    try:
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=path, Body=buffer.getvalue())
        return f'File {path.split("/")[-1]} saved successfully!'
    except Exception as e:
        return f'Error saving file {path.split("/")[-1]}: {str(e)}'

# Process and save data in the bronze layer
def process_bronze(df_bronze):
    file_name_bronze = 'bronze/breweries_bronze.parquet'
    return save_parquet_s3(df_bronze, file_name_bronze)

# Function to process and save data in the silver layer
def process_silver(df_bronze):
    status = 'Silver files saved successfully!'
    try:
        for location, group in df_bronze.groupby('state'):
            file_name_silver = f'silver/{location}/breweries_silver_{location}.parquet'
            save_status = save_parquet_s3(group, file_name_silver)
            if "Erro" in save_status:
                status = save_status
                break
    except Exception as e:
        status = f'Error saving silver files: {str(e)}'
    return status

# Function to process and save data in the gold layer
def process_gold(df_bronze):
    try:
        df_gold = df_bronze.groupby(['brewery_type', 'state']).size().reset_index(name='count')
        file_name_gold = 'gold/breweries_gold.parquet'
        return save_parquet_s3(df_gold, file_name_gold)
    except Exception as e:
        return f'Error saving gold file: {str(e)}'

def lambda_handler(event, context):
    # Search the data
    dados = search_data()

    if dados:
        # Decoding bytes to string
        json_str = dados.decode('utf-8')

        # Load JSON string into a list of dictionaries
        data = json.loads(json_str)

        # Converting a list of dictionaries to a DataFrame
        df_bronze = pd.DataFrame(data)

        # Displaying the DataFrame
        print(df_bronze)

        # Processing the bronze, silver and gold layers
        bronze_status = process_bronze(df_bronze)
        silver_status = process_silver(df_bronze)
        gold_status = process_gold(df_bronze)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'bronze_status': bronze_status,
                'prata_status': silver_status,
                'ouro_status': gold_status
            })
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('Error: Unable to retrieve data.')
        }

# Unit Tests
class TestLambdaHandler(unittest.TestCase):

    @patch('requests.get')
    @patch('boto3.client')
    def test_lambda_handler_success(self, mock_boto_client, mock_requests_get):
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = json.dumps([{'id': '123', 'name': 'Brewery', 'state': 'CA', 'brewery_type': 'micro'}]).encode('utf-8')
        mock_requests_get.return_value = mock_response

        # S3 client mockup
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Executing the lambda_handler function
        result = lambda_handler(None, None)

        # Checking the response
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertIn('bronze_status', body)
        self.assertIn('prata_status', body)
        self.assertIn('ouro_status', body)
        self.assertEqual(body['bronze_status'], 'Bronze file saved successfully!')
        self.assertEqual(body['silver_status'], 'Silver file saved successfully!')
        self.assertEqual(body['gold_status'], 'Gold file saved successfully!')

    @patch('requests.get')
    def test_search_data_success(self, mock_requests_get):
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'[{"id": "123", "name": "Brewery"}]'
        mock_requests_get.return_value = mock_response

        # Running the "search_data" function
        dados = search_data()

        # Verifying that data is returned correctly
        self.assertIsNotNone(dados)
        self.assertEqual(dados, b'[{"id": "123", "name": "Brewery"}]')

    @patch('requests.get')
    def test_search_data_failure(self, mock_requests_get):
        # Mock API response with error
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_requests_get.return_value = mock_response

        # Running the "search_data" function
        dados = search_data()

        # Checking if data is None on error
        self.assertIsNone(dados)

if __name__ == '__main__':
    unittest.main()
