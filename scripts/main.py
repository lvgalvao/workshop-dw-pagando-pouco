import os

from dotenv import load_dotenv
from etl.extract.extract import extract_transaction
from etl.load.load import DeltaLakeSaver
from etl.load.s3_config_model import S3Config
from etl.transformation.tranformation import (
    transform_transaction_table,
)  # Importar a função de transformação
from utility_scripts.logging_utils import log


@log
def main():
    # Carregar as variáveis de ambiente do .env
    load_dotenv()

    # Configuração do S3
    s3_config = S3Config(
        bucket_name=os.getenv("S3_BUCKET_NAME"),
        file_name="transaction",  # Modifique conforme necessário
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_region=os.getenv("AWS_REGION"),
    )

    # Extração de dados
    extracted_data = extract_transaction()

    # Transformação dos dados
    transformed_data = transform_transaction_table(
        extracted_data, datasource_value=os.getenv("DB_NAME")
    )

    # Carregamento dos dados no S3 como Delta Table
    # Assumindo que transformed_data é um pyarrow.Table ou compatível
    saver = DeltaLakeSaver()
    saver.save_to_s3(transformed_data, s3_config)


if __name__ == "__main__":
    main()
