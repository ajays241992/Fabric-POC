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

# ## Renaming DataFrame Columns
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html?highlight=alia
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html?highlight=withcolumnrenamed#pyspark.sql.DataFrame.withColumnRenamed
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnsRenamed.html?highlight=withcolumns#pyspark.sql.DataFrame.withColumnsRenamed

# CELL ********************

# importing functions from the functions and types modules
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_orders = "Files/retail_dataset_de/orders"

df_orders = spark.read.format("parquet").load(path_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# withColumnRenamed
df_orders = df_orders.withColumnRenamed("created_at", "order_timestamp")

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# withColumnRenamed on multiple columns
df_orders = df_orders.withColumnRenamed("order_timestamp","timestamp").withColumnRenamed("unit_price","price")

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# withColumnsRenamed
df_orders = df_orders.withColumnsRenamed({"id":"orderid","user_id":"userid","product_id":"productid"})

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# using alias
df_orders = df_orders.select(col("price").alias("unit_price"))

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
