import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def truncate_table(table_name):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE {table_name};")
        conn.commit()
        print(f"Todas as linhas da tabela '{table_name}' foram excluídas com sucesso.")
    except Exception as e:
        print(f"Erro ao truncar a tabela: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    truncate_table(
        "transactions"
    )  # Substitua 'transactions' pelo nome da tabela que deseja truncar
