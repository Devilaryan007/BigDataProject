create view gold.calender
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/dimensions/calendar_dim/',
    FORMAT ='parquet'
 ) as query1

create view gold.customer
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/dimensions/customer_dim/',
    FORMAT ='parquet'
 ) as query2

create view gold.prod_cat
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/fact/product_category',
    FORMAT ='parquet'
 ) as query5


create view gold.prod_subcat
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/fact/product_subcategory/',
    FORMAT ='parquet'
 ) as query6


create view gold.product
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/fact/products/',
    FORMAT ='parquet'
 ) as query7


create view gold.returns
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/dimensions/returns/',
    FORMAT ='parquet'
 ) as query3


create view gold.sales
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/fact/sales/',
    FORMAT ='parquet'
 ) as query8


create view gold.territories
 as
 select * from OPENROWSET (
    BULK 'https://advrawlake.blob.core.windows.net/silver/dimensions/territories/',
    FORMAT ='parquet'
 ) as query4
