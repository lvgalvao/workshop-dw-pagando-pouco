import pandas as pd
from tools.sql.db.database_connection import getDbConnectionById
from contracts.transactions import Transaction
import os
import datetime
from io import BytesIO
from tools.aws.client import S3Client
import sys


class PostgresCollector:
    def __init__(self, aws_client: S3Client, bdId: int):
        self._envs = {
            "fileName": os.environ.get(f"DB_NAME{bdId}"),
            "tbName": os.environ.get(f"DB_TABLE{bdId}"),
        }
        self._bdId = bdId
        self._model = Transaction
        self._buffer = None
        self._aws = aws_client

        for var in self._envs:
            if self._envs[var] is None:
                print(f"A variável de ambiente {var} não está definida.")
                sys.exit(1)

    def start(self):
        df = self.extract_data()
        print("Processo extract com sucesso")
        df = self.transform_add_columns(df, "postgres")
        print("Processo extract com sucesso")
        self.convert_to_delta(df)

        if self._buffer is not None:
            file_name = self.fileName()
            print(file_name)
            self._aws.upload_file(self._buffer, file_name)
            return True

        return False

    def extract_data(self):
        session = getDbConnectionById(self._bdId)
        query = self.createYesterdayQuery()
        df = pd.read_sql(query, session.bind)
        session.close()
        return df

    def transform_add_columns(self, df, datasource_value):
        df["created_at"] = datetime.datetime.now().isoformat()
        df["datasource"] = datasource_value
        return df

    def convert_to_delta(self, df):
        try:
            # Converte o DataFrame do pandas para uma tabela PyArrow
            self._buffer = BytesIO()
            try:
                df.to_parquet(self._buffer)
                return self._buffer
            except:
                print("Error ao transformar o Df em Parquet")
                self._buffer = None

        except Exception as e:
            print(f"Erro ao converter DataFrame para Delta: {e}")
            self._buffer = None

    def fileName(self):
        data_atual = datetime.datetime.now().isoformat()
        match = data_atual.split(".")
        fileName = self._envs["fileName"]
        return f"postgres/{fileName}-{match[0]}.parquet"

    def createYesterdayQuery(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday = yesterday.strftime("%Y-%m-%d")
        table = self._envs["tbName"]
        query = (
            f"SELECT * FROM {table} WHERE transaction_time >= '{yesterday}'::timestamp"
        )
        return query
