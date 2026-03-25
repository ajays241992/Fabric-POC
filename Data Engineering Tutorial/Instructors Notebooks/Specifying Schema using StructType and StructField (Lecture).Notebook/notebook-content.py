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

# ## Specifying Schema using StructType and StructField
# ### Links and Resources
# - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html
# - https://spark.apache.org/docs/3.5.1/sql-ref-datatypes.html#content

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

df = spark.read.format("csv").\
				option("header", True).\
				schema(schema_users_2).\
				load(path_users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
