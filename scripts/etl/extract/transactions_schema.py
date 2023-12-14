from datetime import datetime

from pydantic import BaseModel


class TransactionBase(BaseModel):
    """
    Esquema base para uma transação.
    """

    id: int = "ID único da transação."
    transaction_id: str = "ID único da transação."
    transaction_time: datetime = "Data e hora da transação."
    ean: str = "Código EAN (European Article Number) do produto."
    product_name: str = "Nome do produto."
    price: float = "Preço do produto."
    store: int = "Número da loja onde a transação ocorreu."

    class Config:
        orm_mode = True


class TransactionCreate(TransactionBase):
    """
    Esquema para criar uma nova transação.
    """

    pass


class TransactionRead(TransactionBase):
    """
    Esquema para ler os detalhes de uma transação existente.
    """

    pos_number: int = "Número do terminal de ponto de venda (POS)."
    pos_system: str = "Nome do sistema de ponto de venda (POS)."
    pos_version: float = "Versão do sistema de ponto de venda (POS)."
    pos_last_maintenance: datetime = (
        "Data da última manutenção do sistema de ponto de venda (POS)."
    )
    operator: int = "ID do operador responsável pela transação."

    class Config:
        orm_mode = True
