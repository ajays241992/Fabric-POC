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

# Importing the libraries

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimeStampType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_reviews = "Files/retail_dataset_de/reviews.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_reviews = spark.read.json(path_reviews)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_reviews)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_users = "Files/retail_dataset_de/users.csv"

schema_users = "id INT, created_at TIMESTAMP, name STRING, email STRING, city STRING, state STRING, zip STRING, birth_date DATE, source STRING"

df_users = spark.read.format("CSV").\
                        option("header","true").\
                        schema(schema_users).\
                        load(path_users)

display(df_users)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
