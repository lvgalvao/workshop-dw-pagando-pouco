# etl_scripts/load/load.py

from abc import ABC, abstractmethod
from io import BytesIO

import boto3
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake
from utility_scripts.logging_utils import log

from .models.s3_config import S3Config  # Importando o S3Config


class S3Saver(ABC):
    @log
    def save_to_s3(self, table, config: S3Config, **kwargs):
        buffer = BytesIO()
        self.write_format(table, buffer, config, **kwargs)

    @abstractmethod
    def write_format(self, table, buffer, config: S3Config, **kwargs):
        pass


class ParquetSaver(S3Saver):
    def write_format(self, table, buffer, config: S3Config, **kwargs):
        pq.write_table(table, buffer)
        self.upload_to_s3(buffer, config)

    def upload_to_s3(self, buffer, config: S3Config):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        s3_client.put_object(
            Bucket=config.bucket_name, Key=config.file_name, Body=buffer.getvalue()
        )


class DeltaLakeSaver(S3Saver):
    def write_format(self, table, buffer, config: S3Config, **kwargs):
        delta_table_uri = f"s3://{config.bucket_name}/{config.file_name}"

        storage_options = {
            "AWS_ACCESS_KEY_ID": config.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": config.aws_secret_access_key,
            "region": config.aws_region,
        }

        write_deltalake(
            data=table,
            table_or_uri=delta_table_uri,
            mode="overwrite",
            overwrite_schema=True,
            storage_options=storage_options,
        )
