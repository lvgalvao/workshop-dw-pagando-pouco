# extract/extract.py
import duckdb
from utility_scripts.logging_utils import log

@log
def extract_csv(file_path):
    con = duckdb.connect(database=':memory:')
    return con.execute(f"SELECT * FROM read_csv_auto('{file_path}', SAMPLE_SIZE=-1)").fetch_arrow_table()
