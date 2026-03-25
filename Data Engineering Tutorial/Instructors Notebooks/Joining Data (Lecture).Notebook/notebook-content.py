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

# ## Joining Data
# ### Links and Resources
# - https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.join.html

# CELL ********************

path_orders = "Files/retail_dataset_de/orders"
df_orders = spark.read.format("parquet").load(path_orders)
display(df_orders.limit(5))

path_products = "Files/retail_dataset_de/products.parquet"
df_products = spark.read.format("parquet").load(path_products)
display(df_products.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# joining df_orders and df_products
df_join = display(df_orders.join(df_products, df_orders["product_id"]==df_products["id"], 'left'))

display(df_join)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = df_orders.\
                join(df_products, df_orders["product_id"]==df_products["id"], 'left').\
                select(
                            df_orders['id'],
                            df_orders['created_at'],
                            df_orders['user_id'],
                            df_orders['quantity'],
                            df_orders['unit_price'], 
                            df_products['title'], 
                            df_products['category'],
                            df_products['vendor']
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
