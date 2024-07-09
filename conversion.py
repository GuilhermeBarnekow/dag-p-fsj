from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os

# Função para converter JSON para Parquet
def convert_json_to_parquet():
    json_path = r"/home/user/Área de Trabalho/1/exportacao.json"
    parquet_path = r"/home/user/Área de Trabalho/1/exportacao.parquet"
    
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        df = pd.DataFrame(data)
        df.to_parquet(parquet_path, index=False)
        print(f"Arquivo convertido com sucesso para {parquet_path}")
    except Exception as e:
        print(f"Ocorreu um erro ao converter o arquivo JSON para Parquet: {e}")

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 7, 7),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    'convert_json_to_parquet',
    default_args=default_args,
    description='Converte arquivo JSON para Parquet',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Tarefa para converter JSON para Parquet
    convert_task = PythonOperator(
        task_id='convert_json_to_parquet',
        python_callable=convert_json_to_parquet,
    )

    # Definindo a ordem de execução das tarefas
convert_task
