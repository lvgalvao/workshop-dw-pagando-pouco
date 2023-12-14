import duckdb
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

    # Carregando os dados no DuckDB a partir de um pyarrow.Table
    con = duckdb.connect(database=":memory:")
    con.register("transactions", arrow_table)
    con.execute("CREATE TABLE transactions AS SELECT * FROM transactions")

    # Lendo os dados de volta em um DataFrame do DuckDB
    duckdb_df = con.execute("SELECT * FROM transactions").fetch_arrow_table()

    con.close()
    return duckdb_df
