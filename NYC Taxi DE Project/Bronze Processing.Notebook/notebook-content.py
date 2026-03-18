# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "557d4890-edf7-4e64-b14f-660c3239e419",
# META       "default_lakehouse_name": "NYC_Lakehouse_DE",
# META       "default_lakehouse_workspace_id": "32157a80-eb85-4a12-93ce-26cb65afdd07",
# META       "known_lakehouses": [
# META         {
# META           "id": "557d4890-edf7-4e64-b14f-660c3239e419"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze Layer Processing

# CELL ********************

# importing libraries
from pyspark.sql.functions import to_timestamp, col, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# parameters
processing_timestamp = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df now is a Spark DataFrame containing parquet data from "Files/nyc-yellow-taxi/landing_files/".
df = spark.read.format("parquet").load("Files/landing-zone/*")

# adding a processing timestamp
df = df.withColumn("processing_timestamp", to_timestamp(lit(processing_timestamp)))

# saving the data in the bronze layer table
# df.write.mode("append").saveAsTable("bronze.nyc_taxi_yellow")
df.write.mode("append").save("Tables/bronze/nyc_taxi_yellow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
