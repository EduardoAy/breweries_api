# Versão 2:
# Código Refatorado, para reutilização de funções (Ex: save_S3, process layers)
# Alteração da Nomeclatura p/ inglês nas funções e variáveis

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

# Obtencao dos dados da API
def search_data():
    response = requests.get("https://api.openbrewerydb.org/breweries")
    if response.status_code == 200:
        return response.content
    else:
        print(f"Erro na requisição: {response.status_code}")
        return None

# Salvar dataFrame como Parquet no S3
def save_parquet_s3(df, path):
    try:
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=path, Body=buffer.getvalue())
        return f'Arquivo {path.split("/")[-1]} salvo com sucesso!'
    except Exception as e:
        return f'Erro ao salvar o arquivo {path.split("/")[-1]}: {str(e)}'

# Processar e salvar os dados na camada bronze
def process_bronze(df_bronze):
    file_name_bronze = 'bronze/breweries_bronze.parquet'
    return save_parquet_s3(df_bronze, file_name_bronze)

# Função para processar e salvar os dados na camada prata
def process_silver(df_bronze):
    status = 'Arquivos prata salvos com sucesso!'
    try:
        for location, group in df_bronze.groupby('state'):
            file_name_silver = f'silver/{location}/breweries_silver_{location}.parquet'
            save_status = save_parquet_s3(group, file_name_silver)
            if "Erro" in save_status:
                status = save_status
                break
    except Exception as e:
        status = f'Erro ao salvar os arquivos prata: {str(e)}'
    return status

# Função para processar e salvar os dados na camada ouro
def process_gold(df_bronze):
    try:
        df_gold = df_bronze.groupby(['brewery_type', 'state']).size().reset_index(name='count')
        file_name_gold = 'gold/breweries_gold.parquet'
        return save_parquet_s3(df_gold, file_name_gold)
    except Exception as e:
        return f'Erro ao salvar o arquivo ouro: {str(e)}'

def lambda_handler(event, context):
    # Buscar os dados
    dados = search_data()

    if dados:
        # Decodificando bytes para string
        json_str = dados.decode('utf-8')

        # Carregar string JSON em uma lista de dicionários
        data = json.loads(json_str)

        # Conversão da lista de dicionários em um DataFrame
        df_bronze = pd.DataFrame(data)

        # Exibindo o DataFrame
        print(df_bronze)

        # Processando as camadas bronze, prata e ouro
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
            'body': json.dumps('Erro: Não foi possível obter os dados.')
        }

# Testes Unitários
class TestLambdaHandler(unittest.TestCase):

    @patch('requests.get')
    @patch('boto3.client')
    def test_lambda_handler_success(self, mock_boto_client, mock_requests_get):
        # Mock da resposta da API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = json.dumps([{'id': '123', 'name': 'Brewery', 'state': 'CA', 'brewery_type': 'micro'}]).encode('utf-8')
        mock_requests_get.return_value = mock_response

        # Mock do client S3
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Execução da função lambda_handler
        result = lambda_handler(None, None)

        # Verificando a resposta
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertIn('bronze_status', body)
        self.assertIn('prata_status', body)
        self.assertIn('ouro_status', body)
        self.assertEqual(body['bronze_status'], 'Arquivo bronze salvo com sucesso!')
        self.assertEqual(body['silver_status'], 'Arquivos prata salvos com sucesso!')
        self.assertEqual(body['gold_status'], 'Arquivo ouro salvo com sucesso!')

    @patch('requests.get')
    def test_search_data_success(self, mock_requests_get):
        # Mock da resposta da API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'[{"id": "123", "name": "Brewery"}]'
        mock_requests_get.return_value = mock_response

        # Executando a função "search_data"
        dados = search_data()

        # Verificando se os dados são retornados corretamente
        self.assertIsNotNone(dados)
        self.assertEqual(dados, b'[{"id": "123", "name": "Brewery"}]')

    @patch('requests.get')
    def test_search_data_failure(self, mock_requests_get):
        # Mock da resposta da API com erro
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_requests_get.return_value = mock_response

        # Executando a função "search_data"
        dados = search_data()

        # Verificando se os dados são None em caso de erro
        self.assertIsNone(dados)

if __name__ == '__main__':
    unittest.main()
