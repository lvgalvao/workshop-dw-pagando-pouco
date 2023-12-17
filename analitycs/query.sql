INSTALL httpfs;
LOAD httpfs;
SET s3_region='us-west-1';
CREATE TABLE transacao AS SELECT * FROM read_parquet('s3://workshop03-salesrecord/postgres/postgres_z8k5-2023-12-17T05:33:54.parquet.parquet');
CREATE OR REPLACE TABLE store AS SELECT * FROM read_parquet('s3://workshop03-salesrecord/loja.parquet');
CREATE TABLE catalogo AS SELECT * FROM read_parquet('s3://workshop03-salesrecord/catalogo.parquet');
SHOW transacao;
SHOW store;
SHOW catalogo;