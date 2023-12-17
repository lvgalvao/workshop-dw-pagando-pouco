from database_setup import DATABASE_URI
from db_connector import connect_to_db, close_connection
from logging_utils import log
from data_saver import copy_csv_to_db


@log
def run_pipeline(loja):
    csv_filename = f"data/daily_sales_retail_{loja}.csv"
    conn = connect_to_db()
    copy_csv_to_db(csv_filename, "transactions", DATABASE_URI)
    close_connection(conn)


if __name__ == "__main__":
    num_lojas = 6  # Número total de lojas

    for loja in range(1, num_lojas + 1):
        run_pipeline(loja)
