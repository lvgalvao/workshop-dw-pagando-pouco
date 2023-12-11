# pipeline.py
from dotenv import load_dotenv
import os
from etl_scripts.extract.extract import extract_csv
from etl_scripts.transform.transform import transform_data
from etl_scripts.load.load import save_to_s3

def main():
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv()

    # Extração
    table = extract_csv('data/Iowa_Liquor_Sales.csv')

    # Transformação
    table_transformed = transform_data(table)

    # Carga
    save_to_s3(
        table_transformed, 
        os.getenv('S3_BUCKET_NAME'), 
        'Iowa_Liquor_Sales_Delta.parquet', 
        os.getenv('AWS_ACCESS_KEY_ID'), 
        os.getenv('AWS_SECRET_ACCESS_KEY')
    )

if __name__ == "__main__":
    main()
