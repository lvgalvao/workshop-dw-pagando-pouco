from abc import ABC, abstractmethod
from io import BytesIO

import pyarrow as pa
from deltalake.writer import write_deltalake
from utility_scripts.logging_utils import log

from .s3_config_model import S3Config

# export AWS_S3_ALLOW_UNSAFE_RENAME=true


class S3Saver(ABC):
    @log
    def save_to_s3(self, data, config: S3Config, **kwargs):
        buffer = BytesIO()
        self.write_format(data, buffer, config, **kwargs)

    @abstractmethod
    def write_format(self, data, buffer, config: S3Config, **kwargs):
        pass


class DeltaLakeSaver(S3Saver):
    def write_format(self, data, buffer, config: S3Config, **kwargs):
        delta_table_uri = f"s3://{config.bucket_name}/{config.file_name}"

        storage_options = {
            "AWS_ACCESS_KEY_ID": config.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": config.aws_secret_access_key,
            "region": config.aws_region,
        }

        # Verificando se os dados já são um pyarrow.Table
        if isinstance(data, pa.Table):
            arrow_table = data
        else:
            raise TypeError("Data must be a pyarrow.Table")

        # Escrevendo para Delta Table
        write_deltalake(
            data=arrow_table,
            table_or_uri=delta_table_uri,
            mode="overwrite",
            overwrite_schema=True,
            storage_options=storage_options,
        )


def load(data, s3_config: S3Config):
    saver = DeltaLakeSaver()
    saver.save_to_s3(data, s3_config)
