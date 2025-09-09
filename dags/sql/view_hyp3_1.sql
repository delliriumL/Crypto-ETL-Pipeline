CREATE OR REPLACE VIEW dm.view_hyp3_1 AS
SELECT
  coin,
  ROUND(AVG(volume), 2) AS avg_volume
FROM dm.dm_hyp3
GROUP BY coin;
