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

# ## Silver Layer Processing

# CELL ********************

# importing libraries
from pyspark.sql.functions import to_timestamp, col, current_timestamp, expr

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

# df now is a Spark DataFrame containing parquet data from the bronze table
df = spark.read.table("bronze.nyc_taxi_yellow") 

# filtering to only process latest batch of data
df = df.filter(f"processing_timestamp = '{processing_timestamp}'")


# sql case statements
vendor_case_sql = """ 
case 
    when VendorID = 1 then 'Creative Mobile Technologies'
    when VendorID = 2 then 'VeriFone'
    else 'Unknown'
end
"""

payment_method_sql = """
case 
    when payment_type = 1 then 'Credit Card'
    when payment_type = 2 then 'Cash'
    when payment_type = 3 then 'No Charge'
    when payment_type = 4 then 'Dispute'
    when payment_type = 5 then 'Unknown'
    when payment_type = 6 then 'Voided Trip'
    else 'Unknown'
end
"""

# using sql case statements to add vendor and payment_method columns
# selecting columns
df = df.\
        withColumn("vendor", expr(vendor_case_sql)).\
        withColumn("payment_method", expr(payment_method_sql)).\
        select(
                "vendor",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "passenger_count",
                "trip_distance",
                col("RatecodeID").alias("ratecode_id"),
                "store_and_fwd_flag",
                col("PULocationID").alias("pu_location_id"),
                col("DOLocationID").alias("do_location_id"),
                "payment_method",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "total_amount",
                "congestion_surcharge",
                col("Airport_fee").alias("airport_fee"),
                "processing_timestamp"
                )

# saving the data in the silver layer table
# df.write.mode("append").saveAsTable("silver.nyc_taxi_yellow")
df.write.mode("append").save("Tables/silver/nyc_taxi_yellow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
