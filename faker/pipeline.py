import random

from data_saver import copy_csv_to_db, save_sales_to_csv
from database_setup import DATABASE_URI
from db_connector import close_connection, connect_to_db
from logging_utils import log
from retail_sales import generate_daily_sales, read_products_from_csv


@log
def run_pipeline(
    file_path, loja, num_days, price_variation_min=-0.2, price_variation_max=0.1
):
    csv_filename = f"data/daily_sales_retail_{loja}.csv"

    conn = connect_to_db()

    if conn:
        products = read_products_from_csv(file_path)
        all_sales = []
        for day in range(num_days):
            price_variation = random.uniform(price_variation_min, price_variation_max)
            daily_sales = generate_daily_sales(products, day, price_variation, loja)
            all_sales.extend(daily_sales)

        save_sales_to_csv(all_sales, csv_filename)

        copy_csv_to_db(csv_filename, "transactions", DATABASE_URI)
        close_connection(conn)


if __name__ == "__main__":
    run_pipeline("data/products_mapping_with_prices.csv", loja=1, num_days=1)
