# use polars backend to process a csv file
def csv_to_parquet [filename]{
sensordata = polars open $filename -d ';' 
| polars query 'SELECT sensor_id, location, lat, lon, timestamp, P1, P2 FROM df WHERE (lat > 51.2) AND (lat < 51.6)'
| polars filter ((polars col lon) < -2.2) 
| polars filter ((polars col lon) > -2.9)
| polars collect


| polars save data/2025-03_sds011_filtered.parquet}