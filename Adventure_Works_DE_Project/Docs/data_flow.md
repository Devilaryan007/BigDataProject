## Data Ingestion

Source data is hosted on GitHub in CSV format.

ADF pipelines:
- Copy Customer.csv
- Copy Product.csv
- Copy Sales.csv

Destination:
ADLS Gen2 Bronze Layer


## Bronze Layer

Raw CSV files copied from GitHub.

## Silver Layer

Data cleansing:
- Null handling
- Data type conversion
- Deduplication

## Gold Layer

Business-ready dimensional model:
- FactSales
- DimCustomer
- DimProduct
- DimDate
