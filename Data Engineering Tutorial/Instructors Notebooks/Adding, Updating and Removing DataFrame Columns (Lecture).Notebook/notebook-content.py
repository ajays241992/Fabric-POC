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

# ## Adding, Updating and Removing DataFrame Columns
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcol#pyspark.sql.DataFrame.withColumn
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

# CELL ********************

# importing functions from the functions and types modules
from pyspark.sql.functions import col, lit, round, concat, current_date, current_timestamp, date_format, to_date, date_format, expr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# loading the orders data into a dataframe
path_orders = "Files/retail_dataset_de/orders"

df_orders = spark.read.format("parquet").load(path_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Adding new columns with the withColumn method

# CELL ********************

# Adding an id2 column that contains the same values as id
df_orders = df_orders.withColumn("id2", col("id"))
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adding multiple withColumn methods
# Replacing an existing column
# Adding a literal column
df_orders = df_orders.\
                    withColumn("id2", col("user_id")).\
                    withColumn("literal_col", lit("hello"))

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# using arithmetic expressions
df_orders = df_orders.\
                    withColumn("order_total", col("unit_price")*col("quantity"))

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# round function
df_orders = df_orders.\
                    withColumn("order_total", round(col("unit_price")*col("quantity"),2))

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# concat function
df_orders = df_orders.\
                    withColumn("row_id", concat(col("id"),col("user_id"), col("product_id")) )

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# current_date and current_timestamp
df_orders = df_orders.\
                    withColumn("current_date", current_date()).\
                    withColumn("current_timestamp", current_timestamp())

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# date_format 
# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df_orders = df_orders.withColumn("formatted_date", date_format(col("created_at"), "dd-MM-yyyy"))
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# to_date 
# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df_orders = df_orders.withColumn("converted_to_date", to_date(col("formatted_date"), "dd-MM-yyyy"))
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# expr demo 1
df_orders = df_orders.withColumn("order_total_2", expr("round(unit_price*quantity,2)"))
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# expr demo 2
df_orders = df_orders.withColumn("literal_col_upper", expr("upper(literal_col)"))
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Removing Columns

# CELL ********************

# dropping a single column with drop
df_orders = df_orders.drop("id2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dropping multiple columns with drop
df_orders = df_orders.drop("literal_col", "order_total", "row_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# using select to retain only the required columns
df_orders = df_orders.select("id", "created_at", "user_id", "product_id", "order_total_2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
