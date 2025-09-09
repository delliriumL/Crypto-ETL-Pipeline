CREATE OR REPLACE VIEW dm.view_hyp2_2 AS
SELECT DISTINCT ON (coin)
    coin,
    date,
    volatility
FROM dm.dm_hyp2
WHERE volatility IS NOT NULL
ORDER BY coin, volatility DESC;
