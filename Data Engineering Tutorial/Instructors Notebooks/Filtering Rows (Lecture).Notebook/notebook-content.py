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

# ## Filtering Rows
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.left.html

# CELL ********************

from pyspark.sql.functions import col, substring

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

# Filtering rows where category is 'Gizmo'

# sql expression
df_products.filter("category = 'Gizmo").show()

# spark column instance
df_products.filter(col("category")=="Gizmo").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtering rows where category is 'Gizmo' AND the 'price' is over 50

# sql expression
df_products.filter("category = 'Gizmo' and price > 50").show()

# spark column instance
df_products.filter(col(("category")=="Gizmo") & (col("price")>50)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtering rows where category is 'Gizmo' OR the 'price' is over 50

# sql expression
df_products.filter("category = 'Gizmo' or price > 50").show()

# spark column instance
df_products.filter(col(("category")=="Gizmo") | (col("price")>50)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtering rows where category begins with 'D'

# sql expression
df_products.filter("left(category,1)= 'D'").show()

# spark column instance
df_products.filter(substring(col(("category"),0,1)=="D")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
