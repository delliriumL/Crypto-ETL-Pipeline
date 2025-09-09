CREATE TABLE dm.dm_hyp1 AS
SELECT
  "Date" AS date,
  'bitcoin' AS coin,
  "Close" - "Open" AS daily_profit
FROM stg.stg_bitcoin
UNION ALL
SELECT
  "Date",
  'ethereum',
  "Close" - "Open"
FROM stg.stg_ethereum
UNION ALL
SELECT
  "Date",
  'dogecoin',
  "Close" - "Open"
FROM stg.stg_dogecoin;
