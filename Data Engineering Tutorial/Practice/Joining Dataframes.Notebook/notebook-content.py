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

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_orders = "Files/retail_dataset_de/orders"
df_orders = spark.read.format("parquet").load(path_orders).limit(5)
display(df_orders)

path_products = "Files/retail_dataset_de/products.parquet"
df_products = spark.read.format("parquet").load(path_products).limit(5)
display(df_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_orders.\
                join(df_products,df_orders["product_id"]==df_products["id"],"left").\
                select(
                    df_orders["id"],
                    df_orders["created_at"],
                    df_orders["user_id"],
                    df_orders["quantity"],
                    df_orders["unit_price"],
                    df_products["category"],
                    df_products["vendor"]
                )

display(df_join)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
