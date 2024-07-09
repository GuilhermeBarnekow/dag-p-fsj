import snowflake.connector
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Definições das variáveis de conexão
account = 'gk08810.sa-east-1.aws'
user_snowflake = 'guilhermebirnfeld'
passphrase = ''
warehouse = 'compute_wh'
database = 'USERS'
schema = 'public'
stage_name = 'usuarios'

# Leitura da chave privada
with open('/home/user/airflow/dags/rsa_key.p8', 'rb') as key_file:
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

# Configuração da conexão com Snowflake
conn = snowflake.connector.connect(
    user=user_snowflake,
    private_key=private_key_bytes,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# Carregar o arquivo Parquet em um DataFrame do Pandas
df = pd.read_parquet(r'/home/user/Área de Trabalho/scripts/exportacao.parquet')

# Criação de um stage temporário para carga
stage_command = f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}"
conn.cursor().execute(stage_command)

# Salvar o DataFrame em um arquivo Parquet localmente
df.to_parquet('temp_parquet_file.parquet')

# Comando PUT para carregar o arquivo Parquet no stage do Snowflake
put_command = f"PUT file://temp_parquet_file.parquet @{stage_name}"
conn.cursor().execute(put_command)

# Copiar dados do stage para a tabela destino
# Assumindo que você tem uma tabela destino chamada 'target_table'
copy_command = f"""
COPY INTO {database}.{schema}.ingest
FROM @{stage_name}/temp_parquet_file.parquet 
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
"""
conn.cursor().execute(copy_command)

# Fechar a conexão com o Snowflake
conn.close()
