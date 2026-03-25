# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "73009411-e4d2-4654-816f-a79e91b478cc",
# META       "default_lakehouse_name": "lakehouse_tutorial_DE",
# META       "default_lakehouse_workspace_id": "79a90b7b-f01a-4946-85b7-e7b75d8aa095",
# META       "known_lakehouses": [
# META         {
# META           "id": "73009411-e4d2-4654-816f-a79e91b478cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

path_products = "Files/retail_dataset_de/products.parquet"

df_products = spark.read.parquet(path_products)

display(df_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.write.mode("overwrite").format("delta").save("Files/writing_demo/products_delta")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.format("delta").load("Files/writing_demo/products_delta"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.write.mode("overwrite").format("parquet").saveAsTable("retail_analytics.products_csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table("retail_analytics.products"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
