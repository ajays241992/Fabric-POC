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

# ## Reading Parquet Files
# ### Links and Resources
# - https://spark.apache.org/docs/latest/
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html

# MARKDOWN ********************

# ### Reading a single parquet file

# CELL ********************

# Reading the parquet file into a DataFrame

spark.read.parquet("Files/retail_dataset_de/products.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The Type function can confirm this is a DataFrame object

type(spark.read.parquet("Files/retail_dataset_de/products.parquet"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The show method displays the DataFrame to the console

spark.read.parquet("Files/retail_dataset_de/products.parquet").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The display function renders the DataFrame as rich, interactive HTML tables

display(spark.read.parquet("Files/retail_dataset_de/products.parquet"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Storing the DataFrame in the variable df
# Storing the file path in a variable path

path = "Files/retail_dataset_de/products.parquet"

df = spark.read.parquet(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading multiple parquet files

# CELL ********************

# Reading from the parquet folder

path_orders = "Files/retail_dataset_de/orders"

df_orders = spark.read.parquet(path_orders)

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Alternate syntax

path_orders = "Files/retail_dataset_de/orders"

df_orders = spark.read.format("parquet").load(path_orders)

display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
