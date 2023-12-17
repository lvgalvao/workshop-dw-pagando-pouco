import csv
import random
from datetime import datetime, timedelta

from faker import Faker


def generate_product_mapping_with_prices(num_items, min_price=2.0, max_price=100.0):
    faker = Faker()
    mapping = {}
    for _ in range(num_items):
        product_ean = faker.unique.ean13()
        product_name = faker.word().capitalize()
        price = round(random.uniform(min_price, max_price), 2)
        mapping[product_ean] = {"Product Name": product_name, "Price": price}
    return mapping


def read_products_from_csv(file_path):
    products = []
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            products.append(
                {
                    "EAN": row["EAN"],
                    "Product Name": row["Product Name"],
                    "Price": float(row["Price"]),
                }
            )
    return products


def generate_transaction_time(day, start_hour=9, end_hour=17):
    hour = random.randint(start_hour, end_hour)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime.now().replace(
        hour=hour, minute=minute, second=second, microsecond=0
    ) + timedelta(days=day)


def generate_daily_sales(
    products,
    day,
    price_variation,
    store,
    num_sales_range=(350, 450),
    products_per_sale_range=(5, 50),
    quantity_per_product_range=(1, 20),
):
    faker = Faker()
    sales_data = []
    num_sales = random.randint(*num_sales_range)

    # Definindo valores fixos para Pos system e Pos version
    pos_system = "Windows"
    pos_version = 2.0
    pos_last_maintenance = datetime.now() - timedelta(days=90)  # 3 meses atrás

    for _ in range(num_sales):
        num_products_in_sale = random.randint(*products_per_sale_range)
        transaction_time = generate_transaction_time(day)
        transaction_id = faker.uuid4()  # Usando Faker para gerar o ID da transação

        pos_number = random.randint(1, 20)
        operator = random.randint(1, 60)

        for _ in range(num_products_in_sale):
            product = random.choice(products)
            quantity = random.randint(*quantity_per_product_range)
            adjusted_price = round(product["Price"] * (1 + price_variation), 2)

            for _ in range(quantity):
                sales_data.append(
                    {
                        "Transaction ID": str(
                            transaction_id
                        ),  # Convertendo UUID para string
                        "Transaction Time": transaction_time,
                        "EAN": product["EAN"],
                        "Product Name": product["Product Name"],
                        "Price": adjusted_price,
                        "Store": store,
                        "Pos Number": pos_number,
                        "Pos System": pos_system,
                        "Pos Version": pos_version,
                        "Pos Last Maintenance": pos_last_maintenance,
                        "Operator": operator,
                    }
                )

    return sales_data


def save_sales_to_csv(sales_data, filename):
    sales_data.sort(key=lambda x: x["Transaction Time"], reverse=True)
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "transaction_id",
                "transaction_time",
                "ean",
                "product_name",
                "price",
                "store",
                "pos_number",
                "pos_system",
                "pos_version",
                "pos_last_maintenance",
                "operator",
            ]
        )
        for sale in sales_data:
            writer.writerow(
                [
                    sale["Transaction ID"],
                    sale["Transaction Time"],
                    sale["EAN"],
                    sale["Product Name"],
                    sale["Price"],
                    sale["Store"],
                    sale["Pos Number"],
                    sale["Pos System"],
                    sale["Pos Version"],
                    sale["Pos Last Maintenance"],
                    sale["Operator"],
                ]
            )
    print(f"Dados de vendas salvos em '{filename}'")


if __name__ == "__main__":
    # Gerando mapeamento de 200 itens com preços
    # expanded_product_mapping_with_prices = generate_product_mapping_with_prices(200)

    # # Salvando os dados em um arquivo CSV
    # with open('data/products_mapping_with_prices.csv', 'w', newline='', encoding='utf-8') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(['EAN', 'Product Name', 'Price'])  # Cabeçalho do CSV

    #     for ean, details in expanded_product_mapping_with_prices.items():
    #         writer.writerow([ean, details['Product Name'], details['Price']])

    # print("Dados salvos em 'products_mapping_with_prices.csv'")

    products = read_products_from_csv("data/products_mapping_with_prices.csv")
    for loja in range(8, 15):
        all_sales = []
        for day in range(10):
            price_variation = random.uniform(-1.0, 2.0)
            daily_sales = generate_daily_sales(products, day, price_variation, loja)
            all_sales.extend(daily_sales)
        save_sales_to_csv(all_sales, f"data/daily_sales_retail_{loja}.csv")
