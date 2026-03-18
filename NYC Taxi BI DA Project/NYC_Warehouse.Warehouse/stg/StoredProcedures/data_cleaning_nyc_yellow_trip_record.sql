CREATE PROCEDURE stg.data_cleaning_nyc_yellow_trip_record
@end_date DATETIME2,
@start_date DATETIME2
AS
DELETE from stg.nyc_taxi_yellow_trip where tpep_pickup_datetime < @start_date OR tpep_pickup_datetime > @end_date;