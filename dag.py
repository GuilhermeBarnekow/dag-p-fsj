from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import json
import logging
from pymongo import MongoClient
import snowflake.connector
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import io
import os

# Variáveis de conexão MongoDB
ATLAS_URI = ""
DB_NAME = "prodvenc"
COLLECTION_NAME = "prodvenc"

# Variáveis de conexão Snowflake
account = ''
user_snowflake = ''
passphrase = ''
warehouse = ''
database = ''
schema = ''
stage_name = ''
private_key_path = r''

# Caminhos dos arquivos temporários
TEMP_JSON_PATH = '/tmp/temp_data.json'
TEMP_PARQUET_PATH = '/tmp/temp_data.parquet'

# Função para extrair dados do MongoDB e salvar como JSON em arquivo temporário
def extract_mongo_data_to_file():
    logging.info("Iniciando a extração dos dados do MongoDB.")
    try:
        client = MongoClient(ATLAS_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        data = list(collection.find())
        logging.info(f"Número de documentos extraídos do MongoDB: {len(data)}")

        with open(TEMP_JSON_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, default=str, ensure_ascii=False, indent=4)

        client.close()
        logging.info(f"Dados extraídos e salvos em {TEMP_JSON_PATH} com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao extrair dados do MongoDB: {str(e)}")
        raise

# Função para converter JSON para Parquet e salvar em arquivo temporário
def convert_json_to_parquet():
    logging.info("Iniciando a conversão de JSON para Parquet.")
    try:
        with open(TEMP_JSON_PATH, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logging.info(f"Número de registros carregados do JSON: {len(data)}")

        df = pd.json_normalize(data)
        logging.info(f"Número de colunas no DataFrame: {len(df.columns)}")

        df.to_parquet(TEMP_PARQUET_PATH, index=False)
        logging.info(f"Dados convertidos para Parquet e salvos em {TEMP_PARQUET_PATH} com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao converter JSON para Parquet: {str(e)}")
        raise

# Função para carregar dados Parquet para Snowflake
def load_parquet_to_snowflake():
    logging.info("Iniciando o carregamento dos dados para o Snowflake.")
    try:
        with open(private_key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase.encode(),
                backend=default_backend()
            )

        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            user=user_snowflake,
            private_key=private_key_bytes,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        logging.info("Conexão com Snowflake estabelecida com sucesso.")

        stage_command = f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}"
        conn.cursor().execute(stage_command)
        logging.info(f"Stage temporário {stage_name} criado.")

        put_command = f"PUT file://{TEMP_PARQUET_PATH} @{stage_name}"
        conn.cursor().execute(put_command)
        logging.info(f"Arquivo Parquet carregado para o stage {stage_name}.")

        copy_command = f"""
        COPY INTO {database}.{schema}.bronze
        FROM @{stage_name}/temp_data.parquet
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
        conn.cursor().execute(copy_command)
        logging.info("Dados copiados do stage para a tabela de destino no Snowflake.")

        conn.close()
        logging.info("Conexão com Snowflake fechada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao carregar dados no Snowflake: {str(e)}")
        raise

default_args = {
    'owner': 'admin',
    'start_date': pendulum.today('UTC').add(days=-1),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'export_mongo_and_load_to_snowflake',
    default_args=default_args,
    description='Extrair dados do MongoDB e carregar para Snowflake',
    schedule='@daily',
    catchup=False
)

extract_mongo_data_task = PythonOperator(
    task_id='extract_mongo_data_to_file',
    python_callable=extract_mongo_data_to_file,
    dag=dag,
)

convert_json_to_parquet_task = PythonOperator(
    task_id='convert_json_to_parquet',
    python_callable=convert_json_to_parquet,
    dag=dag,
)

load_parquet_task = PythonOperator(
    task_id='load_parquet_to_snowflake',
    python_callable=load_parquet_to_snowflake,
    dag=dag,
)

extract_mongo_data_task >> convert_json_to_parquet_task >> load_parquet_task
