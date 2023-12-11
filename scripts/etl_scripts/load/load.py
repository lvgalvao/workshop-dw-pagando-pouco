# load/load.py
import boto3
from io import BytesIO
import pyarrow.parquet as pq
from utility_scripts.logging_utils import log

@log
def save_to_s3(table, bucket_name, file_name, aws_access_key_id, aws_secret_access_key):
    # Convertendo para Parquet usando PyArrow
    buffer = BytesIO()
    pq.write_table(table, buffer)

    # Configuração do cliente S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Salvar no S3
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())
