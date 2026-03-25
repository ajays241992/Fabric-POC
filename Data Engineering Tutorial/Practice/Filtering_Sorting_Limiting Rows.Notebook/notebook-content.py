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

from pyspark.sql.functions import col,asc,desc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_products= "Files/retail_dataset_de/products.parquet"

df_products = spark.read.parquet(path_products)

display(df_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.filter(col('category') == 'Gizmo').show()

display(df_products)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.where("category = 'Gizmo' or price > 50").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.sort(col("price")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.sort(col("category").asc(), col("price").desc()).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products.sort(col("price").desc()).limit(10).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
