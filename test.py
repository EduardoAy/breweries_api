# Arquivo de testes
import json
import pytest
import boto3
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler, buscar_dados, bucket_name

@patch('requests.get')
def test_buscar_dados_success(mock_requests_get):
    # Mock da resposta da API
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'[{"id": "123", "name": "Brewery"}]'
    mock_requests_get.return_value = mock_response

    # Executando a função buscar_dados
    dados = buscar_dados()

    # Verificando se os dados são retornados corretamente
    assert dados is not None
    assert dados == b'[{"id": "123", "name": "Brewery"}]'

@patch('requests.get')
def test_buscar_dados_failure(mock_requests_get):
    # Mock da resposta da API com erro
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_requests_get.return_value = mock_response

    # Executando a função buscar_dados
    dados = buscar_dados()

    # Verificando se os dados são None em caso de erro
    assert dados is None

@patch('requests.get')
@patch('boto3.client')
def test_lambda_handler_success(mock_boto_client, mock_requests_get):
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
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert 'bronze_status' in body
    assert 'prata_status' in body
    assert 'ouro_status' in body
    assert body['bronze_status'] == 'Arquivo bronze salvo com sucesso!'
    assert body['prata_status'] == 'Arquivos prata salvos com sucesso!'
    assert body['ouro_status'] == 'Arquivo ouro salvo com sucesso!'

if __name__ == '__main__':
    pytest.main()


# Explicação dos Testes

# test_buscar_dados_success: Testa a função buscar_dados, garantindo que os dados sejam retornados corretamente quando a API responde com sucesso.
# test_buscar_dados_failure: Testa a função buscar_dados, para garantir que retorne None quando a API responde com erro.
# test_lambda_handler_success: Testa a função lambda_handler quando a API retorna uma resposta bem-sucedida, para verificar se os arquivos são salvos corretamente em todas as camadas (bronze, prata e ouro).
