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

# ## Writing DataFrames to Files
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html

# CELL ********************

products_path = "Files/retail_dataset_de/products.parquet"

products_df = spark.read.parquet(products_path)

display(products_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Writing to Files

# CELL ********************

products_df.\
write.\
# option can be overwrite, append, ignore or error
mode("overwrite").\ 
# option is specific to csv and text format
option("header", "true")
# choose the format
format("csv").\
save("Files/writing_demo/products.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.\
write.\
# mode can be overwrite, append, ignore or error
mode("append").\
# choose the format
format("parquet").\
save("Files/writing_demo/products.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_df.\
write.\
# option can be overwrite, append, ignore or error
mode("overwrite").\ 
# choose the format
format("delta").\
save("Files/writing_demo/products_delta")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
