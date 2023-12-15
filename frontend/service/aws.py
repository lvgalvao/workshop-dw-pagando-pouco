import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
CATALOGO_S3_PATH = os.getenv("CATALOGO_S3_PATH")


def upload_to_aws(data):
    # Converte os dados para DataFrame
    df = pd.DataFrame(data)

    # Salvando como parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Criar cliente S3
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try:
        # Fazendo o upload
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME, Key="catalogo/catalogo.parquet", Body=buffer
        )
        print("Upload concluído com sucesso!")
    except Exception as e:
        print(f"Erro ao fazer upload: {e}")
