# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

# Azure Data Lake Storage authentication
# Configure credentials securely using Databricks Secret Scope.

storage_account = "<storage-account>"
tenant_id = dbutils.secrets.get(
    scope="<secret-scope>",
    key="<tenant-id-key>"
)
client_id = dbutils.secrets.get(
    scope="<secret-scope>",
    key="<client-id-key>"
)
client_secret = dbutils.secrets.get(
    scope="<secret-scope>",
    key="<client-secret-key>"
)

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)
# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC #### Readable Data

# COMMAND ----------

# df = spark.read.format("csv")\
#         .option("header", "true")\
#         .option("inferSchema", "true")\
#         .load('abfss://bronze@adventureworkdatalake01.dfs.core.windows.net/ADWCalendar')

# COMMAND ----------


# Bronze container path
base_path = "abfss://bronze@adventureworkdatalake01.dfs.core.windows.net/"

# List all folders in the Bronze container
items = dbutils.fs.ls(base_path)

folders = [item.path for item in items if item.isDir()]

print(f"Total folders found: {len(folders)}")
for folder in folders:
    print(folder)

# COMMAND ----------


# Dictionary to store each dataset separately
dataframes = {}

for folder_path in folders:
    folder_name = folder_path.rstrip("/").split("/")[-1]

    try:
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(folder_path)
        )

        dataframes[folder_name] = df

        print(f"Loaded: {folder_name}")

    except Exception as e:
        print(f"Could not load {folder_name}: {e}")

print(f"\nSuccessfully loaded {len(dataframes)} datasets.")

# COMMAND ----------


# # Display all rows from every loaded dataset

# for name, df in dataframes.items():
#     print(f"Dataset: {name}")
#     print(f"Total rows: {df.count()}")
#     print(f"Total columns: {len(df.columns)}")
    
#     display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender

# COMMAND ----------

display(dataframes["ADWCalendar"])

# COMMAND ----------

ADWCalendar = dataframes["ADWCalendar"].withColumn('month', month(col('Date')))\
                                       .withColumn('year', year(col('Date')))

# COMMAND ----------

ADWCalendar.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWCalendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

display(dataframes["ADWCustomers"])

# COMMAND ----------

# DBTITLE 1,Customers FullName
ADWCustomer = dataframes["ADWCustomers"].withColumn('FullName', concat(col('prefix'),lit(' '),col('FirstName'),lit(' '), col('LastName')))
ADWCustomer.display()

# COMMAND ----------

ADW_cust = dataframes["ADWCustomers"].withColumn('FullName',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
ADW_cust.display()

# COMMAND ----------

ADW_cust.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWCustomers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Categories

# COMMAND ----------

dataframes["ADWPCategories"].write.format('parquet')\
                                  .mode('append')\
                                  .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWPCategories")\
                                  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sub Categories

# COMMAND ----------

display(dataframes["ADWPSubcategories"])

# COMMAND ----------

dataframes["ADWPSubcategories"].write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWPSubcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

display(dataframes["ADWProducts"])

# COMMAND ----------

ADW_Prod = dataframes["ADWProducts"].withColumn('ProductSKU',split(col('ProductSKU'),'-')[0],)\
                                    .withColumn('ProductName',split(col('ProductName'),' ')[0],)
ADW_Prod.display()

# COMMAND ----------

ADW_Prod.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWProducts")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns

# COMMAND ----------

display(dataframes["ADWReturns"])

# COMMAND ----------

dataframes["ADWReturns"].write.format('parquet')\
                        .mode('append')\
                        .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWReturns")\
                        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

display(dataframes["ADWTerritories"])

# COMMAND ----------

dataframes["ADWTerritories"].write.format('parquet')\
                            .mode('append')\
                            .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWTerritories")\
                            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Transformation

# COMMAND ----------

dataframes["ADWSales15"].display()

# COMMAND ----------

df_Sales = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .load('abfss://bronze@adventureworkdatalake01.dfs.core.windows.net/ADWSales*')

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales = df_Sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_Sales = df_Sales.withColumn('OrderNumber',regexp_replace('orderNumber','S','T'))

# COMMAND ----------

df_Sales = df_Sales.withColumn('multiply',col('OrderLineItem') * col("OrderQuantity"))

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales.write.format('parquet')\
              .mode('append')\
              .option("path", "abfss://silver@adventureworkdatalake01.dfs.core.windows.net/ADWSales")\
              .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Analysis

# COMMAND ----------

df_Sales.groupBy('OrderDate').agg(count("OrderNumber").alias('total_order')).display()

# COMMAND ----------

dataframes["ADWPCategories"].display()

# COMMAND ----------

dataframes['ADWTerritories'].display()