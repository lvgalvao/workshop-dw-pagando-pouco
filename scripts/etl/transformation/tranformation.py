import pyarrow as pa
from datetime import datetime
from utility_scripts.logging_utils import log


# Função para transformar o esquema da tabela
@log
def transform_transaction_table(table, datasource_value):
    # Adicione a coluna "created_at" diretamente à tabela PyArrow
    created_at_array = pa.array([datetime.now()] * len(table))
    table = table.append_column("created_at", created_at_array)

    # Adicione a coluna "datasource" diretamente à tabela PyArrow
    datasource_array = pa.array([datasource_value] * len(table))
    table = table.append_column("datasource", datasource_array)

    return table
