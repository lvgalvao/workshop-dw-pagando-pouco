# transaction_model.py
from sqlalchemy import Column, Date, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, name="transaction_id")
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
