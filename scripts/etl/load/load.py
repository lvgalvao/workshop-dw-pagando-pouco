# from abc import ABC, abstractmethod
# from io import BytesIO
# import pyarrow as pa
# from deltalake.writer import write_deltalake
# from utility_scripts.logging_utils import log
# from .s3_config_model import S3Config


# class SaveToS3(ABC):
#     def __init__(self, saver_type):
#         self.saver_type = saver_type

#     def save_to_s3(self, data, config: S3Config, **kwargs):
#         buffer = BytesIO()
#         self.write_format(data, buffer, config, **kwargs)

#     @abstractmethod
#     def write_format(self, data, buffer, config: S3Config, **kwargs):
#         pass


# class DeltaLakeSaver(SaveToS3):
#     @log
#     def write_format(self, data, buffer, config: S3Config, **kwargs):
#         delta_table_uri = f"s3://{config.bucket_name}/{config.file_name}"
#         storage_options = {
#             "AWS_ACCESS_KEY_ID": config.aws_access_key_id,
#             "AWS_SECRET_ACCESS_KEY": config.aws_secret_access_key,
#             "region": config.aws_region,
#         }
#         if not isinstance(data, pa.Table):
#             raise TypeError("Data must be a pyarrow.Table")
#         pa.write_deltalake(
#             data=data,
#             table_or_uri=delta_table_uri,
#             mode="overwrite",
#             overwrite_schema=True,
#             storage_options=storage_options,
#         )


# # class IcebergSaver(SaveToS3):
# #     @log
# #     def write_format(self, data, buffer, config: S3Config, **kwargs):
# #         iceberg_table_uri = f"s3://{config.bucket_name}/{config.file_name}"
# #         storage_options = {
# #             "AWS_ACCESS_KEY_ID": config.aws_access_key_id,
# #             "AWS_SECRET_ACCESS_KEY": config.aws_secret_access_key,
# #             "region": config.aws_region,
# #         }
# #         if not isinstance(data, pa.Table):
# #             raise TypeError("Data must be a pyarrow.Table")

# #         # Criar uma tabela Iceberg e escrever os dados
# #         iceberg_table = (iceberg_table_uri, storage_options=storage_options)
# #         iceberg_table.write(data)


# # Função para carregar os dados no S3 com o tipo de saver especificado
# def load(data, s3_config: S3Config, saver_type):
#     if saver_type == "delta":
#         saver = DeltaLakeSaver(saver_type)
#     elif saver_type == "iceberg":
#         saver = IcebergSaver(saver_type)
#     else:
#         raise ValueError("Invalid saver_type")

#     saver.save_to_s3(data, s3_config)
