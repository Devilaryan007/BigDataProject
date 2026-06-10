# Databricks notebook source
# MAGIC %md
# MAGIC # silver layer code

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.advrawlake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.advrawlake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.advrawlake.dfs.core.windows.net", "96f86591-1b4e-4a8b-8e4b-d14fdd8aab0b")
spark.conf.set("fs.azure.account.oauth2.client.secret.advrawlake.dfs.core.windows.net", "yhU8Q~5yYs7ouDL-ggwXp.6fdzUZWNTUGCJAYcuo")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.advrawlake.dfs.core.windows.net", "https://login.microsoftonline.com/a3e7d672-264e-4915-b013-aa3b2f3a9db0/oauth2/token")

# COMMAND ----------

df_products=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Products.csv")

# COMMAND ----------

df_calender=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Calendar.csv")
df_customers=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Customers.csv")
df_product_cat=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Product_Categories.csv")
df_product_subcat=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Product_Subcategories.csv")
df_returns=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Returns.csv")
df_sales=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Sales*.csv")
df_territories=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Territories.csv")

# COMMAND ----------

df_calender.display()

# COMMAND ----------

from pyspark.sql.functions import *
df_calender = df_calender.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))
df_calender = df_calender.withColumn("Year", year(col("Date")))
df_calender = df_calender.withColumn("Month", month(col("Date")))
df_calender = df_calender.withColumn("Day", dayofmonth(col("Date")))
df_calender = df_calender.withColumn("Week", weekofyear(col("Date")))
df_calender = df_calender.withColumn("DayofWeek", dayofweek(col("Date")))
df_calender = df_calender.withColumn("DayofYear", dayofyear(col("Date")))
df_calender = df_calender.withColumn("Quarter", quarter(col("Date")))
df_calender = df_calender.withColumn("IsWeekend", when(col("DayofWeek") == 1, 0).when(col("DayofWeek") == 7, 0).otherwise(1))

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.functions import *

display(
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("abfss://bronze@advrawlake.dfs.core.windows.net/datasets/Calendar.csv")
        .withColumn("Year", year(col("Date")))
        .withColumn("Month", month(col("Date")))
        .withColumn("Day", dayofmonth(col("Date")))
        .withColumn("Week", weekofyear(col("Date")))
        .withColumn("DayofWeek", dayofweek(col("Date")))
        .withColumn("DayofYear", dayofyear(col("Date")))
        .withColumn("Quarter", quarter(col("Date")))
        .withColumn("IsWeekend", when(col("DayofWeek") == 1, 0).when(col("DayofWeek") == 7, 0).otherwise(1))
        .withColumn("IsHoliday", lit(0))
        .withColumn("IsWeekday", when(col("DayofWeek") == 1, 0).when(col("DayofWeek") == 7, 0).otherwise(1))
)

# COMMAND ----------

df_calender.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/dimensions/calendar_dim")


# COMMAND ----------

df_customers=df_customers.withColumn("Customer_name",concat_ws(" ",col("Prefix"),col("FirstName"),col("LastName")))
df_customers.display()

# COMMAND ----------

df_customers.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/dimensions/customer_dim")

# COMMAND ----------

df_product_subcat.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/fact/product_subcategory")

# COMMAND ----------

df_products=df_products.withColumn("ProductSKU",split(col("ProductSKU"),"-")[0]).withColumn("ProductName",split(col("ProductName")," ")[0])
df_products.display()

# COMMAND ----------

df_products.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/fact/products")

# COMMAND ----------

# DBTITLE 1,Cell 14
df_returns.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/dimensions/returns")

# COMMAND ----------

df_territories.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/dimensions/territories")

# COMMAND ----------

df_product_cat.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/fact/product_category")

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales=df_sales.withColumn("StockDate", to_timestamp(col("StockDate"),"MM/dd/yyyy")).withColumn("OrderNumber", regexp_replace(col("OrderNumber"),"S", "T")).withColumn('multiply', col("OrderLineItem") * col("OrderQuantity"))
df_sales.write.mode("append").format("parquet").save("abfss://silver@advrawlake.dfs.core.windows.net/fact/sales")


# COMMAND ----------

df_sales.groupBy("OrderDate").agg(count("OrderNumber").alias("total_count")).display()

# COMMAND ----------

df_product_subcat.display()
