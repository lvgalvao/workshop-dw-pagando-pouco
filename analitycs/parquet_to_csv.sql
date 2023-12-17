CREATE TABLE catalogao AS SELECT * FROM read_parquet ('loja.parquet');
COPY catalogao TO 'catalogao.csv' DELIMITER ',' CSV HEADER;
SELECT * FROM catalogao;

.mode markdown
.output catalogao.md