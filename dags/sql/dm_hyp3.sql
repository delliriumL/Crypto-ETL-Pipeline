CREATE TABLE dm.dm_hyp3 AS
SELECT
  "Date" AS date,
  'bitcoin' AS coin,
  "Volume" AS volume
FROM stg.stg_bitcoin
UNION ALL
SELECT
  "Date",
  'ethereum',
  "Volume"
FROM stg.stg_ethereum
UNION ALL
SELECT
  "Date",
  'dogecoin',
  "Volume"
FROM stg.stg_dogecoin;
