CREATE OR REPLACE VIEW dm.view_hyp3_2 AS
SELECT *
FROM (
  SELECT *,
         RANK() OVER (PARTITION BY coin ORDER BY volume DESC) AS rnk
  FROM dm.dm_hyp3
) t
WHERE rnk = 1;
