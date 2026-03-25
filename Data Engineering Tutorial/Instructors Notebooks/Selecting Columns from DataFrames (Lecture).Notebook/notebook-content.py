# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6f891a7e-aab7-4e91-a4ee-786056851776",
# META       "default_lakehouse_name": "TutorialLakehouse",
# META       "default_lakehouse_workspace_id": "c0a6c243-f3d9-4994-9b88-5172c1d903b8"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Selecting Columns from DataFrames
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html?highlight=select#pyspark.sql.DataFrame.select
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.col.html?highlight=col#pyspark.sql.functions.col

# CELL ********************

# importing the col function
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Selecting 3 columns
df = df.select("id", "quantity", "unit_price")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Wildcard to select all columns
df = df.select("*")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Selecting columns
df = df.select(df["id"], df["quantity"], df["unit_price"])

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Selecting columns with alternate syntax and aliasing unit_price
df = df.select(df["id"], df["quantity"], df["unit_price"].alias("price"))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Selecting columns using the col function
df = df.select(col("id"), col("quantity"), col("unit_price"))


display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Selecting columns using the col function and aliasing unit_price
df = df.select(col("id"), col("quantity"), col("unit_price").alias("price"))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading the orders data to a DataFrame
path_orders = "Files/retail_dataset_de/orders"

df = spark.read.parquet(path_orders)

# Selecting columns using the dataframe dot notation and aliasing unit_price
df = df.select(df.id, df.quantity, df.unit_price.alias("price"))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
