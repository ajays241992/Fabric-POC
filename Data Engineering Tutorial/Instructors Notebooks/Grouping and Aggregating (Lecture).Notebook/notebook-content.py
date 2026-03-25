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

# ## Grouping and Aggregating
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions

# CELL ********************

from pyspark.sql.functions import sum, avg, round

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_orders = 'Files/retail_dataset_de/orders'

df_orders = spark.read.format("parquet").load(path_orders)

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# groupBy
df_orders.groupBy("user_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sum aggregation
df_orders.groupBy("user_id").sum("quantity").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# grouping by multiple columns
df_orders.groupBy("user_id", "product_id").sum("quantity").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sorting
df_orders.\
        groupBy("user_id", "product_id").\
        sum("quantity").\
        sort("sum(quantity)", ascending=False).\
        show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# renaming an aggregated column
df_orders.\
        groupBy("user_id", "product_id").\
        sum("quantity").\
        withColumnRenamed("sum(quantity", "total_quantity").\
        sort("total_quantity", ascending=False).\
        show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# using the agg method to perform multiple aggregations
df_orders.\
        groupBy("user_id").\
        agg(
            sum("quantity").alias("total_quantity"),
            avg("quantity").alias("avg_quantity") 
            ).\
        sort("total_quantity", ascending=False).\
        show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# rounding avg_quantity to 2dp
df_orders.\
        groupBy("user_id").\
        agg(
            sum("quantity").alias("total_quantity"),
            avg("quantity").alias("avg_quantity") 
            ).\
        withColumn("avg_quantity", round("avg_quantity", 2)).\
        sort("total_quantity", ascending=False).\
        show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
