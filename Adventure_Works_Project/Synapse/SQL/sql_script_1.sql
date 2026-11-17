CREATE DATABASE SCOPED CREDENTIAL cred_ADWorks
WITH IDENTITY = 'Managed Identity'

-- CREATE EXTERNAL DATA SOURCE source_silver
-- WITH
-- (
--    LOCATION = 'https://adventureworkdatalake01.dfs.core.windows.net/silver',
--    CREDENTIAL = cred_ADWorks
-- )

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
   LOCATION = 'https://adventureworkdatalake01.dfs.core.windows.net/gold',
   CREDENTIAL = cred_ADWorks
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


---------------------------------
------CREATE EXTERNAL TABLE-------
-------------EXTSALES-------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * from gold.sales_15_16_17;

select * from gold.extsales

-- ================= Calendar Table ======================

CREATE EXTERNAL TABLE gold.extcalender
WITH
(
    LOCATION = 'extcalender',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.calender

-- ================= Customer Table ======================

CREATE EXTERNAL TABLE gold.extcustomer
WITH
(
    LOCATION = 'extcustomer',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.customer

-- ================= Product Table ======================

CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION = 'extproduct',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.Products

-- ================= ProductCategories Table ======================

CREATE EXTERNAL TABLE gold.extprodcat
WITH
(
    LOCATION = 'extproductCategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.ProdCat

-- ================= ProductSubCategories Table ======================

CREATE EXTERNAL TABLE gold.extprodsubcat
WITH
(
    LOCATION = 'extproductSubCategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.ProdSubCat

-- ================= Product Returns Table ======================

CREATE EXTERNAL TABLE gold.extreturns
WITH
(
    LOCATION = 'extproreturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.ret_urns

-- ================= Territories Table ======================

CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.Territories