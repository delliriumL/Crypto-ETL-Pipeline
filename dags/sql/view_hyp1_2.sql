CREATE OR REPLACE VIEW dm.view_hyp1_2 AS
SELECT *
FROM (
  SELECT *,
         RANK() OVER (PARTITION BY coin ORDER BY daily_profit DESC) AS rnk
  FROM dm.dm_hyp1
) t
WHERE rnk = 1;
