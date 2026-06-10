--MASTER KEY CREATION
------------------------------------------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Aryan@007'

--CREDENTIAL CREATION
------------------------------------------------------
CREATE DATABASE SCOPED CREDENTIAL sourav_cred
with IDENTITY = 'Managed Identity'

--EXTERNAL DATA SOURCE CREDENTIAL
------------------------------------------------------
CREATE EXTERNAL DATA SOURCE src_silver with (
    LOCATION = 'https://advrawlake.blob.core.windows.net/silver',
    CREDENTIAL = sourav_cred
)


CREATE EXTERNAL DATA SOURCE tgt_gold with (
    LOCATION = 'https://advrawlake.blob.core.windows.net/gold',
    CREDENTIAL = sourav_cred
)

--EXTERNAL FILE CREATION
------------------------------------------------------
CREATE EXTERNAL FILE FORMAT format_parquet
WITH(
    FORMAT_TYPE= PARQUET,
    DATA_COMPRESSION ='org.apache.hadoop.io.compress.SnappyCodec'

)


--EXTERNAL TABLE CREATION
------------------------------------------------------
CREATE EXTERNAL TABLE gold.customer_ext
WITH
(
    LOCATION = 'extcustomer',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
)
as SELECT * from gold.customer;




CREATE EXTERNAL TABLE gold.product_ext
WITH
(
    LOCATION = 'extproduct',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.product;


CREATE EXTERNAL TABLE gold.sales_ext
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.sales;


CREATE EXTERNAL TABLE gold.returns_ext
WITH
(
    LOCATION = 'extreturns',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
)AS SELECT * FROM gold.returns;


CREATE EXTERNAL TABLE gold.territories_ext
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
)AS SELECT * FROM gold.territories;


CREATE EXTERNAL TABLE gold.prod_cat_ext
WITH
(
    LOCATION = 'extprodcat',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
)AS SELECT * FROM gold.prod_cat;



CREATE EXTERNAL TABLE gold.prod_subcat_ext
WITH
(
    LOCATION = 'extprodsubcat',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
)AS SELECT * FROM gold.prod_subcat;



CREATE EXTERNAL TABLE gold.calender_ext
WITH
(
    LOCATION = 'extcalender',
    DATA_SOURCE = tgt_gold,
    FILE_FORMAT = format_parquet
)AS SELECT * FROM gold.calender;
