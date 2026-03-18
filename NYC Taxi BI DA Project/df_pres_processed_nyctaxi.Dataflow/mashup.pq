[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"stg nyc_taxi_yellow_trip" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "41bb86f1-2097-45f4-88fb-50995c6fd220"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "26306c76-3fec-406d-8d93-049ba59b37ec"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "stg", Item = "nyc_taxi_yellow_trip"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"RatecodeID", "store_and_fwd_flag", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "Airport_fee"}),
  #"Inserted conditional column" = Table.AddColumn(#"Removed columns", "vendor", each if [VendorID] = 1 then "Creative Mobile Technologies, LLC" else if [VendorID] = 2 then "Curb Mobility, LLC" else if [VendorID] = 6 then "Myle Technologies Inc" else if [VendorID] = 7 then "Helix" else "Unknown"),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted conditional column", {"VendorID", "vendor", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "payment_type", "total_amount"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Reordered columns", {"VendorID"}),
  #"Inserted conditional column 1" = Table.AddColumn(#"Removed columns 1", "payment", each if [payment_type] = 0 then "Flex Fare trip" else if [payment_type] = 1 then "Credit card" else if [payment_type] = 2 then "Cash" else if [payment_type] = 3 then "No charge" else if [payment_type] = 4 then "Dispute" else if [payment_type] = 5 then "Unknown" else if [payment_type] = 6 then "Voided trip" else "Other"),
  #"Removed columns 2" = Table.RemoveColumns(#"Inserted conditional column 1", {"payment_type"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Removed columns 2", {"vendor", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "payment", "total_amount"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns 1", {{"PULocationID", "pu_locationid"}, {"DOLocationID", "do_locationid"}}),
  #"Calculated date" = Table.TransformColumns(#"Renamed columns", {{"tpep_dropoff_datetime", each DateTime.Date(_), type nullable date}, {"tpep_pickup_datetime", each DateTime.Date(_), type nullable date}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Calculated date", {{"payment", type text}, {"vendor", type text}})
in
  #"Changed column type";
shared #"stg taxi_zone_lookup" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "41bb86f1-2097-45f4-88fb-50995c6fd220"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "26306c76-3fec-406d-8d93-049ba59b37ec"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "stg", Item = "taxi_zone_lookup"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"LocationID", Int64.Type}})
in
  #"Changed column type";
shared Merge = let
  Source = Table.NestedJoin(#"stg nyc_taxi_yellow_trip", {"pu_locationid"}, #"stg taxi_zone_lookup", {"LocationID"}, "stg taxi_zone_lookup", JoinKind.LeftOuter),
  #"Expanded stg taxi_zone_lookup" = Table.ExpandTableColumn(Source, "stg taxi_zone_lookup", {"Borough", "Zone"}, {"Borough", "Zone"}),
  #"Reordered columns" = Table.ReorderColumns(#"Expanded stg taxi_zone_lookup", {"vendor", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "pu_locationid", "Borough", "Zone", "do_locationid", "payment", "total_amount"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Borough", "pu_borough"}, {"Zone", "pu_zone"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"pu_locationid"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge (2)_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "vendor", DestinationColumnName = "vendor"], [SourceColumnName = "tpep_pickup_datetime", DestinationColumnName = "tpep_pickup_datetime"], [SourceColumnName = "tpep_dropoff_datetime", DestinationColumnName = "tpep_dropoff_datetime"], [SourceColumnName = "pu_borough", DestinationColumnName = "pu_borough"], [SourceColumnName = "pu_zone", DestinationColumnName = "pu_zone"], [SourceColumnName = "do_borough", DestinationColumnName = "do_borough"], [SourceColumnName = "do_zone", DestinationColumnName = "do_zone"], [SourceColumnName = "payment_method", DestinationColumnName = "payment_method"], [SourceColumnName = "passenger_count", DestinationColumnName = "passenger_count"], [SourceColumnName = "trip_distance", DestinationColumnName = "trip_distance"], [SourceColumnName = "total_amount", DestinationColumnName = "total_amount"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared #"Merge (2)" = let
  Source = Table.NestedJoin(Merge, {"do_locationid"}, #"stg taxi_zone_lookup", {"LocationID"}, "stg taxi_zone_lookup", JoinKind.LeftOuter),
  #"Expanded stg taxi_zone_lookup" = Table.ExpandTableColumn(Source, "stg taxi_zone_lookup", {"Borough", "Zone"}, {"Borough", "Zone"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded stg taxi_zone_lookup", {{"Borough", "do_borough"}, {"Zone", "do_zone"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"vendor", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "pu_borough", "pu_zone", "do_borough", "do_zone", "do_locationid", "payment", "total_amount"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"do_locationid"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Removed columns", {"vendor", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pu_borough", "pu_zone", "do_borough", "do_zone", "payment", "passenger_count", "trip_distance", "total_amount"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Reordered columns 1", {{"payment", "payment_method"}})
in
  #"Renamed columns 1";
shared #"Merge (2)_DataDestination" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41bb86f1-2097-45f4-88fb-50995c6fd220"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "26306c76-3fec-406d-8d93-049ba59b37ec"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "nyctaxi_yellow"]}[Data]
in
  TableNavigation;
