from pydantic import BaseModel


class S3Config(BaseModel):
    bucket_name: str
    file_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
