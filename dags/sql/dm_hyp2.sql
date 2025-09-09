CREATE TABLE dm.dm_hyp2 AS
SELECT
  "Date" AS date,
  'bitcoin' AS coin,
  "High" / NULLIF("Low", 0) AS volatility
FROM stg.stg_bitcoin
UNION ALL
SELECT
  "Date",
  'ethereum',
  "High" / NULLIF("Low", 0)
FROM stg.stg_ethereum
UNION ALL
SELECT
  "Date",
  'dogecoin',
  "High" / NULLIF("Low", 0)
FROM stg.stg_dogecoin;
