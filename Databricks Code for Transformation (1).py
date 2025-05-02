# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC code below will ask for a few things
# MAGIC 1) storage account
# MAGIC 2) application id
# MAGIC 3) Servise credential 
# MAGIC 4) Directory id

# COMMAND ----------

# original code from site step 7 : https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage

#service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

#spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
#spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

storage_account = "<storage_account_name>"
application_id = "<your_appication_id>"
directory_id = "<your_directory_id>"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "jsJ8Q~aNwp~RpkHsChvdnRO1wZFvXVzgbods3bhL")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")


# COMMAND ----------

# now we are ready to read the data

# COMMAND ----------

# MAGIC %md
# MAGIC abfss = azure blob file system storage
# MAGIC
# MAGIC olistdata is where bronz, silver and gold folder are stored
# MAGIC

# COMMAND ----------

# reading "bronz/olist_customers_dataset.csv" from container "olistdata" from storage account "olistdatastorageaccnsk" with path "/bronz/olist_customers_dataset.csv"
customer_df = spark.read.\
format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load(f"abfss://olistdata@<storage_account_name>.dfs.core.windows.net/bronz/olist_customers_dataset.csv")

display(customer_df)

# COMMAND ----------

base_path = "abfss://olistdata@<storage_account_name>.dfs.core.windows.net/bronz/"
#.load(f"abfss://olistdata@olistdatastorageaccnsk.dfs.core.windows.net/bronz/olist_customers_dataset.csv")

orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").load(payments_path)
reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)


# COMMAND ----------

# enriching by mongobd server data


# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "spnca.h.filess.io"
database = "<database_name>"
port = "<port_number>"
username = "<username>"
password = "<mongoDB_db_password>"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

collection = mydatabase['product_category'] # "product_category" is collection name in mongodb database (colab noteboo)
# we use it to connect to mongodb database

import pandas as pd
mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

mongo_data

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff, current_date, when

# COMMAND ----------

def clean_dtaframe(df,name):
    print("Cleaning " + name)
    return df.dropDuplicates().na.drop("all")

orders_df = clean_dtaframe(orders_df,"Orders")
display(orders_df)

# COMMAND ----------

# Convert Date Columns

dorders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
        .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

# COMMAND ----------

#Calculating delivery & time delays
orders_df = orders_df.withColumn("actual_delivery_time", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
orders_df = orders_df.withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
orders_df = orders_df.withColumn("Delay Time", col("actual_delivery_time") - col("estimated_delivery_time"))

display(orders_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Joining

# COMMAND ----------

order_customer_df = orders_df.join(customer_df, orders_df.customer_id == customer_df.customer_id, "left")

order_payment_df = order_customer_df.join(payments_df, order_customer_df.order_id == payments_df.order_id, "left")

order_items_df = order_payment_df.join(items_df, 'order_id', "left")

order_products_df = order_items_df.join(products_df, order_items_df.product_id == products_df.product_id, "left")

final_df = order_products_df.join(sellers_df, order_products_df.seller_id == sellers_df.seller_id, "left")

# in this final df geolocation data and review data is not joined yet


# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data

# COMMAND ----------

# joining data from mongoDB in the final dataframe
# fist we will have to convert the mongo data into spark dataframe

# dropping the _id column
mongo_data.drop("_id", axis = 1, inplace = True)

# Converting into spark dataframe
mongo_spark_df = spark.createDataFrame(mongo_data)

display(mongo_spark_df)

# COMMAND ----------

final_df = final_df.join(mongo_spark_df,"product_category_name","left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns

    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df = remove_duplicate_columns(final_df)


# COMMAND ----------

# storing the data into data strage-silver file
final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastorageaccnsk.dfs.core.windows.net/silver")

# link to learn about different modes : https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html

# COMMAND ----------

  