# pipeline.py

from dotenv import load_dotenv
import os
from etl_scripts.extract.extract import extract_csv
from etl_scripts.transform.transform import transform_data
from etl_scripts.load.load import DeltaLakeSaver
from etl_scripts.load.models.s3_config import S3Config  # Importando o S3Config
from utility_scripts.logging_utils import log

@log
def main():
    load_dotenv()

    # Configurações do S3
    s3_config = S3Config(
        bucket_name=os.getenv('S3_BUCKET_NAME'),
        file_name='Iowa_Liquor_Sales_Delta',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_region=os.getenv('AWS_REGION')
    )

    # Extração
    table = extract_csv('data/Iowa_Liquor_Sales.csv')

    # Transformação
    table_transformed = transform_data(table)

    # Escolha entre DeltaLakeSaver ou ParquetSaver conforme necessário
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    delta_saver = DeltaLakeSaver()

    # Carga no formato Delta Lake
    delta_saver.save_to_s3(table_transformed, s3_config)

if __name__ == "__main__":
    main()
