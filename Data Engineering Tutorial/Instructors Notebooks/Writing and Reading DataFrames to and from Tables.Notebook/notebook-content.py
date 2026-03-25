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

# ## Writing and Reading DataFrames to and from Tables (Lectures)
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html

# CELL ********************

# Loading the products data into a DataFrame products_df
products_df = spark.read.parquet("Files/retail_dataset_de/products.parquet")

display(products_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Writing DataFrames to Tables

# CELL ********************

products_df.\
write.\
# mode can be overwrite, append, ignore or error
mode("overwrite").\
# if you don't qualify the table it will be reference the dbo schema
saveAsTable("TutorialLakehouseDE.dbo.products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.\
write.\
# mode can be overwrite, append, ignore or error
mode("overwrite").\
# default format is delta
format("delta").\
# if you don't qualify the table it will be reference the dbo schema
saveAsTable("TutorialLakehouseDE.retail_analytics.products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading Tables into DataFrames

# CELL ********************

# by default the products table from the dbo schema will be loaded
display(spark.read.table("products"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# specifying the lakehouse and schema
display(spark.read.table("TutorialLakehouseDE.retail_analytics.products"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
