-- BITCOIN
ALTER TABLE stg.bitcoin
  RENAME COLUMN "Date" TO date,
  RENAME COLUMN "Open" TO open,
  RENAME COLUMN "High" TO high,
  RENAME COLUMN "Low" TO low,
  RENAME COLUMN "Close" TO close,
  RENAME COLUMN "Volume" TO volume;

DELETE FROM stg.bitcoin
WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL;

-- ETHEREUM
ALTER TABLE stg.ethereum
  RENAME COLUMN "Date" TO date,
  RENAME COLUMN "Open" TO open,
  RENAME COLUMN "High" TO high,
  RENAME COLUMN "Low" TO low,
  RENAME COLUMN "Close" TO close,
  RENAME COLUMN "Volume" TO volume;

DELETE FROM stg.ethereum
WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL;

-- DOGECOIN
ALTER TABLE stg.dogecoin
  RENAME COLUMN "Date" TO date,
  RENAME COLUMN "Open" TO open,
  RENAME COLUMN "High" TO high,
  RENAME COLUMN "Low" TO low,
  RENAME COLUMN "Close" TO close,
  RENAME COLUMN "Volume" TO volume;

DELETE FROM stg.dogecoin
WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL;
