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
    # Carregar dados do CSV para a tabela no banco de dados
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Mapear os nomes das colunas do CSV para os nomes das colunas da classe Transaction
    column_mapping = {
        "Pos Number": "pos_number",
        "Pos System": "pos_system",
        "Pos Version": "pos_version",
        "Pos Last Maintenance": "pos_last_maintenance",
        "Operator": "operator",
        "Transaction ID": "transaction_id",
        "Transaction Time": "transaction_time",
        "EAN": "ean",
        "Product Name": "product_name",
        "Price": "price",
        "Store": "store",
    }

    # No loop que lê as linhas do CSV, faça a correspondência das colunas
    with open(csv_filename, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Remapeie os nomes das colunas do CSV para os nomes das colunas da classe Transaction
            mapped_row = {column_mapping[key]: value for key, value in row.items()}

            # Faça a conversão das datas e horas aqui, se necessário

            # Crie uma instância do modelo Transaction com os dados do CSV mapeados
            transaction = Transaction(**mapped_row)
            session.add(transaction)

    session.commit()
    session.close()

    print(f"Dados de vendas copiados para a tabela '{table_name}' no banco de dados")
