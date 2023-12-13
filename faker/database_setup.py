import os

from dotenv import load_dotenv
from sqlalchemy import (Column, Date, DateTime, Float, Integer, String,
                        create_engine)
from sqlalchemy.orm import declarative_base

load_dotenv()

DATABASE_URI = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
Base = declarative_base()


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(
        Integer, primary_key=True, autoincrement=True
    )  # Nova coluna id como chave primária
    transaction_id = Column(String, name="transaction_id")  # Removido primary_key=True
    transaction_time = Column(DateTime, name="transaction_time")
    ean = Column(String, name="ean")
    product_name = Column(String, name="product_name")
    price = Column(Float, name="price")
    store = Column(Integer, name="store")
    pos_number = Column(Integer, name="pos_number")
    pos_system = Column(String, name="pos_system")
    pos_version = Column(Float, name="pos_version")
    pos_last_maintenance = Column(Date, name="pos_last_maintenance")
    operator = Column(Integer, name="operator")


def create_tables():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    create_tables()
