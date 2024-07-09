from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
from pymongo import MongoClient

ATLAS_URI = "mongodb://guilherme.barnekow:ZQSBh0LRQ1bXFHbEhX9W@192.168.100.223:27017,192.168.100.224:27017,192.168.100.225:27017/prodvenc?authSource=admin&readPreference=secondary&replicaSet=hmlRepl"
DB_NAME = "prodvenc"
COLLECTION_NAME = "prodvenc"
EXPORT_PATH = r"/home/user/Área de Trabalho/1/exportacao.json"

def extract_and_export_mongo_data():
    client = MongoClient(ATLAS_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    data = list(collection.find())
    
    with open(EXPORT_PATH, 'w', encoding='utf-8') as file:
        json.dump(data, file, default=str, ensure_ascii=False, indent=4)

    client.close()

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 7, 7),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'export_mongo_to_json',
    default_args=default_args,
    description='Extrair dados da base',
    schedule_interval='@daily',
    catchup=False,
)

extract_and_export_task = PythonOperator(
    task_id='extract_and_export_mongo_data',
    python_callable=extract_and_export_mongo_data,
    dag=dag,
)

extract_and_export_task
