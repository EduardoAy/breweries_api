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

# Função para obter os dados da API
def buscar_dados():
    response = requests.get("https://api.openbrewerydb.org/breweries")
    if response.status_code == 200:
        return response.content
    else:
        print(f"Erro na requisição: {response.status_code}")
        return None

def lambda_handler(event, context):
    # Buscar os dados
    dados = buscar_dados()

    if dados:
        # Decodificando bytes para string
        json_str = dados.decode('utf-8')

        # Carregando a string JSON em uma lista de dicionários
        data = json.loads(json_str)

        # Convertendo a lista de dicionários em um DataFrame
        df_bronze = pd.DataFrame(data)

        # Exibindo o DataFrame
        print(df_bronze)
        
        # Bronze - Convertendo o DataFrame para Parquet
        try:
            table_bronze = pa.Table.from_pandas(df_bronze)
            buffer_bronze = io.BytesIO()
            pq.write_table(table_bronze, buffer_bronze)
            buffer_bronze.seek(0)

            # Nome do arquivo, incluindo o caminho da pasta
            file_name_bronze = 'bronze/breweries_bronze.parquet'
            
            # Salvando o arquivo no S3
            s3.put_object(Bucket=bucket_name, Key=file_name_bronze, Body=buffer_bronze.getvalue())
            bronze_status = 'Arquivo bronze salvo com sucesso!'
        except Exception as e:
            bronze_status = f'Erro ao salvar o arquivo bronze: {str(e)}'
        
        # Prata - Agrupando o DataFrame por 'brewery location'
        prata_status = 'Arquivos prata salvos com sucesso!'
        try:
            for location, group in df_bronze.groupby('state'):
                # Convertendo o grupo para Parquet
                table_prata = pa.Table.from_pandas(group)
                buffer_prata = io.BytesIO()
                pq.write_table(table_prata, buffer_prata)
                buffer_prata.seek(0)

                # Nome do arquivo incluindo o caminho da pasta
                file_name_prata = f'prata/{location}/breweries_prata.parquet'
                
                # Salvando o arquivo no S3
                s3.put_object(Bucket=bucket_name, Key=file_name_prata, Body=buffer_prata.getvalue())
        except Exception as e:
            prata_status = f'Erro ao salvar os arquivos prata: {str(e)}'
        
        # Ouro - Criando visão agregada com a quantidade de cervejarias por tipo e localização
        try:
            # Agrupando os dados por 'brewery_type' e 'state'
            df_ouro = df_bronze.groupby(['brewery_type', 'state']).size().reset_index(name='count')
            
            # Convertendo o DataFrame para Parquet
            table_ouro = pa.Table.from_pandas(df_ouro)
            buffer_ouro = io.BytesIO()
            pq.write_table(table_ouro, buffer_ouro)
            buffer_ouro.seek(0)

            # Nome do arquivo incluindo o caminho da pasta
            file_name_ouro = 'ouro/breweries_ouro.parquet'
            
            # Salvando o arquivo no S3
            s3.put_object(Bucket=bucket_name, Key=file_name_ouro, Body=buffer_ouro.getvalue())
            ouro_status = 'Arquivo ouro salvo com sucesso!'
        except Exception as e:
            ouro_status = f'Erro ao salvar o arquivo ouro: {str(e)}'

        return {
            'statusCode': 200,
            'body': json.dumps({'bronze_status': bronze_status, 'prata_status': prata_status, 'ouro_status': ouro_status})
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

        # Mock do cliente S3
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Executando a função lambda_handler
        result = lambda_handler(None, None)

        # Verificando se a resposta é correta
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertIn('bronze_status', body)
        self.assertIn('prata_status', body)
        self.assertIn('ouro_status', body)
        self.assertEqual(body['bronze_status'], 'Arquivo bronze salvo com sucesso!')
        self.assertEqual(body['prata_status'], 'Arquivos prata salvos com sucesso!')
        self.assertEqual(body['ouro_status'], 'Arquivo ouro salvo com sucesso!')

    @patch('requests.get')
    def test_buscar_dados_success(self, mock_requests_get):
        # Mock da resposta da API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'[{"id": "123", "name": "Brewery"}]'
        mock_requests_get.return_value = mock_response

        # Executando a função buscar_dados
        dados = buscar_dados()

        # Verificando se os dados são retornados corretamente
        self.assertIsNotNone(dados)
        self.assertEqual(dados, b'[{"id": "123", "name": "Brewery"}]')

    @patch('requests.get')
    def test_buscar_dados_failure(self, mock_requests_get):
        # Mock da resposta da API com erro
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_requests_get.return_value = mock_response

        # Executando a função buscar_dados
        dados = buscar_dados()

        # Verificando se os dados são None em caso de erro
        self.assertIsNone(dados)

if __name__ == '__main__':
    unittest.main()



# Explicação dos Testes
# test_buscar_dados_success: Testa a função buscar_dados para garantir que os dados sejam retornados corretamente quando a API responde com sucesso.
# test_buscar_dados_failure: Testa a função buscar_dados para garantir que retorne None quando a API responde com erro.
# test_lambda_handler_success: Testa a função lambda_handler quando a API retorna uma resposta bem-sucedida e verifica se os arquivos são salvos corretamente em todas as camadas (bronze, prata e ouro).
