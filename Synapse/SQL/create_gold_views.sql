-----------------------
-- create view Calender
-----------------------
CREATE VIEW gold.Territories
AS
SELECT
    *
FROM
    OPENROWSET(BULK 'https://adventureworkdatalake01.dfs.core.windows.net/silver/ADWTerritories/',
               FORMAT = 'PARQUET'
              ) as ter