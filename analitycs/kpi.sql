CREATE VIEW resultado AS SELECT 
    t.store, 
    AVG(100 * ABS(t.price - p.Preço) / p.Preço) AS DiferencaPercentualMedia
FROM 
    transacao t
JOIN 
    catalogo p ON t.ean = p.EAN
GROUP BY 
    t.store
ORDER BY 
    DiferencaPercentualMedia DESC
LIMIT 1;