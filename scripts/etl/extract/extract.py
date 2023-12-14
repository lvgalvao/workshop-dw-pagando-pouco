import pyarrow as pa
from utility_scripts.logging_utils import log

from ..db.database_connection import Session
from ..db.transactions_model import Transaction


@log
def extract_transaction():
    session = Session()
    data = session.query(Transaction).all()
    session.close()

    # Convertendo os objetos SQLAlchemy em um dicionário
    # com listas correspondentes a cada coluna
    data_dict = {col: [] for col in Transaction.__table__.columns.keys()}
    for obj in data:
        for col in data_dict.keys():
            data_dict[col].append(getattr(obj, col))

    # Convertendo o dicionário em um pyarrow.Table
    arrow_table = pa.Table.from_pydict(data_dict)
    return arrow_table
