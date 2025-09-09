CREATE OR REPLACE VIEW dm.view_hyp1_1 AS
SELECT
  coin,
  ROUND(AVG(daily_profit)::numeric, 4) AS avg_profit
FROM dm.dm_hyp1
GROUP BY coin;
