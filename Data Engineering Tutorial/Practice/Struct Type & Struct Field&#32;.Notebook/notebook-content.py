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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_users = "Files/retail_dataset_de/users.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using StructType and StructField

schema_users_2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("source", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_users = spark.read.format("csv").\
                        option("header","true").\
                        schema(schema_users_2).\
                        load(path_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
