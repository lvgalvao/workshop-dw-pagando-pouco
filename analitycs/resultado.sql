SELECT * FROM resultado;
COPY resultado TO 'kpi.csv' WITH CSV HEADER DELIMITER ',';
