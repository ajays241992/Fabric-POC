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

# ## Gold Layer Processing


# CELL ********************

# importing libraries
from pyspark.sql.functions import to_timestamp, col, current_timestamp, expr, date_format

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

# reading the nyc_taxi_yellow data (for the latest processed batch) into a dataframe df
df = spark.read.table("silver.nyc_taxi_yellow").filter(f"processing_timestamp = '{processing_timestamp}'")

# reading the lookup data into a dataframe df_pu_lookup
df_pu_lookup = spark.read.table("silver.taxi_zone_lookup")

# reading the lookup data into a dataframe df_do_lookup
df_do_lookup = spark.read.table("silver.taxi_zone_lookup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# joining df with df_po_lookup to get pickup information
df = df.join(df_pu_lookup, df["pu_location_id"]==df_pu_lookup["LocationID"], "left")

# joining df with df_do_lookup to get dropoff information        
df = df.join(df_do_lookup, df["do_location_id"]==df_do_lookup["LocationID"], "left")


# selecting only required columns from df
# performing transformations
df = df.\
        select(
                "vendor", 
                # changing to standard date format and aliasing columns
                date_format("tpep_pickup_datetime","yyyy-MM-dd").alias("pickup_date"), 
                date_format("tpep_dropoff_datetime","yyyy-MM-dd").alias("dropoff_date"), 
                df_pu_lookup["Borough"].alias("pickup_borough"), 
                df_do_lookup["Borough"].alias("dropoff_borough"), 
                df_pu_lookup["Zone"].alias("pickup_zone"), 
                df_do_lookup["Zone"].alias("dropoff_zone"), 
                "payment_method", 
                "passenger_count", 
                "trip_distance", 
                "tip_amount", 
                "total_amount",
                "processing_timestamp" )

# writing to gold table
#df.write.mode("append").saveAsTable("gold.nyc_taxi_yellow")

df.write.mode("append").save("Tables/gold/nyc_taxi_yellow")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
