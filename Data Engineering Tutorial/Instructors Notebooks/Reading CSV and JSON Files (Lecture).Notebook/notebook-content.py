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

# ## Reading CSV and JSON Files
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html
# - https://spark.apache.org/docs/3.5.1/sql-ref-datatypes.html#content
# - https://spark.apache.org/docs/3.5.1/sql-data-sources-csv.html
# - https://spark.apache.org/docs/3.5.1/sql-data-sources-json.html

# MARKDOWN ********************

# ### Reading CSV Files

# CELL ********************

# This will assign string data type to each column and header as false

path_users = "Files/retail_dataset_de/users.csv"

df_users = spark.read.format("csv").load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Specifying the header option as true

path_users = "Files/retail_dataset_de/users.csv"

df_users = spark.read.option("header","true").format("csv").load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Specifying the schema using ddl

path_users = "Files/retail_dataset_de/users.csv"

schema_users = "id INT, created_at TIMESTAMP, name STRING, email STRING, city STRING, state STRING, zip STRING, birth_date DATE, source STRING"

# Spreading the code across multiple lines
df_users = spark.read.option("header","true").\
                        schema(schema_users).\
                        format("csv").\
                        load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inferring the schema

path_users = "Files/retail_dataset_de/users.csv"

# Spreading the code across multiple lines
df_users = spark.read.option("header","true").\
                        option("inferSchema", "true").\
                        format("csv").\
                        load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adding the seperator option


path_users = "Files/retail_dataset_de/users.csv"

schema_users = "id INT, created_at TIMESTAMP, name STRING, email STRING, city STRING, state STRING, zip STRING, birth_date DATE, source STRING"

# Spreading the code across multiple lines
df_users = spark.read.option("header","true").\
                        option("sep",","),\
                        schema(schema_users).\
                        format("csv").\
                        load(path_users)

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading JSON Files

# CELL ********************

# Reading a single line JSON file

path_reviews = "Files/retail_dataset_de/reviews.json"

df_reviews = spark.read.format("json").load(path_reviews)

display(df_reviews)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
