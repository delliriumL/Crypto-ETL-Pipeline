CREATE OR REPLACE VIEW dm.view_hyp2_1 AS
SELECT
  coin,
  ROUND(AVG(volatility)::numeric, 4) AS avg_volatility
FROM dm.dm_hyp2
GROUP BY coin;
