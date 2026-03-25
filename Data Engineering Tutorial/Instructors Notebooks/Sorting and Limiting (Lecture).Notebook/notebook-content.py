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

# ## Sorting and Limiting
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.asc.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.desc.html

# CELL ********************

from pyspark.sql.functions import col, asc, desc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_products = "Files/retail_dataset_de/products.parquet"

df_products = spark.read.format("parquet").load(path_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sorting in ascending price order

# implicit
df_products.sort(col("category")).show()

# explicit
df_products.sort(col("category"), ascending=True).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sorting in descending price order

df_products.sort(col("category"), ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sorting multiple columns in ascending or descending order with asc and desc

display(df_products.sort(col("category").asc(), col("price").desc()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using limit and sort to display top 10 products by highest price

df_products.sort(col("price").desc()).limit(10).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
