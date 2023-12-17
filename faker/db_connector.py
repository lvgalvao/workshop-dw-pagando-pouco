import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("2_DB_NAME"),
            user=os.getenv("2_DB_USER"),
            password=os.getenv("2_DB_PASSWORD"),
            host=os.getenv("2_DB_HOST"),
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def close_connection(conn):
    conn.close()
