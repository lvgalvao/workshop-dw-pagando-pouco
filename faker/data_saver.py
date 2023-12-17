import csv

from database_setup import Transaction
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def save_sales_to_csv(sales_data, filename):
    sales_data.sort(key=lambda x: x["Transaction Time"], reverse=True)
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "Pos Number",
                "Pos System",
                "Pos Version",
                "Pos Last Maintenance",
                "Operator",
                "Transaction ID",
                "Transaction Time",
                "EAN",
                "Product Name",
                "Price",
                "Store",
            ]
        )
        for sale in sales_data:
            writer.writerow(
                [
                    sale["Pos Number"],
                    sale["Pos System"],
                    sale["Pos Version"],
                    sale["Pos Last Maintenance"],
                    sale["Operator"],
                    sale["Transaction ID"],
                    sale["Transaction Time"],
                    sale["EAN"],
                    sale["Product Name"],
                    sale["Price"],
                    sale["Store"],
                ]
            )
    print(f"Dados de vendas salvos em '{filename}'")


def copy_csv_to_db(csv_filename, table_name, conn_str):
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    session = Session()

    with open(csv_filename, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            transaction_data = {
                "pos_number": row["pos_number"],
                "pos_system": row["pos_system"],
                "pos_version": float(row["pos_version"]),
                "pos_last_maintenance": row["pos_last_maintenance"],
                "operator": int(row["operator"]),
                "transaction_id": row["transaction_id"],
                "transaction_time": row["transaction_time"],
                "ean": row["ean"],
                "product_name": row["product_name"],
                "price": float(row["price"]),
                "store": int(row["store"]),
            }
            transaction = Transaction(**transaction_data)
            session.add(transaction)

    session.commit()
    session.close()
    print(f"Dados de vendas copiados para a tabela '{table_name}' no banco de dados")
