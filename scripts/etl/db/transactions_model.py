# transaction_model.py
from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, name="transaction_id")
    transaction_time = Column(DateTime, name="transaction_time")
    ean = Column(String, name="ean")
    price = Column(Float, name="price")
    store = Column(Integer, name="store")
