
wget -r -np -nH --cut-dirs=3 -R "index.html*" -A "2024*_sds011.zip" https://archive.sensor.community/csv_per_month/


	2023-01_ds18b20.zip
./duckdb ld.duckdb
-- use ctr+shift+enter to send to console. Set in keybindings.json
-- inspect the polars cleaned subset of the data
CREATE OR REPLACE table tdf_tbl as SELECT * FROM read_csv_auto('data/tdf100k.csv');

-- make a table from the raw data by setting column types and ignoring errors - we lose a few rows but that's ok

CREATE OR REPLACE table ld_all_clean_tbl as SELECT * FROM read_csv('data/2024-01_sds011.csv',
delim = ";",
header = true,
columns = {
    'sensor_id': BIGINT,
    'sensor_type': VARCHAR,
    'location': VARCHAR,
    'lat': FLOAT,
    'lon': FLOAT,
    'timestamp': TIMESTAMP,
    'P1': FLOAT,
    'durP1': FLOAT,
    'ratioP1': FLOAT,
    'P2': FLOAT,
    'durP2': FLOAT,
    'ratioP2': FLOAT
},
ignore_errors = true);

COPY ld_all_clean_tbl TO 'data/ld_all_clean_tbl.parquet' (FORMAT PARQUET);

DESCRIBE ld_all_clean_tbl;
SELECT COUNT(*) FROM ld_all_clean_tbl;

-- create a view of the data that is in the West of England area
-- CREATE OR REPLACE view lep_ld_view as SELECT sensor_id, location, lat, lon, timestamp, P1 as pm10, P2 as pm25  FROM ld_all_clean_tbl WHERE lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18;

SELECT count(*) FROM lep_ld_view;

-- subset to West of england, with lat and lon criteria, group by hour (ending) and sensor_id and calculate the mean hourly pm10 and pm2.5. Retain lat and long for the sensor.
CREATE OR REPLACE view lep_ld_view as SELECT 
    sensor_id,
    lat,
    lon, 
    DATE_TRUNC('hour', timestamp) + INTERVAL 1 HOUR as hour, 
    AVG(pm10) as pm10, 
    AVG(pm25) as 'pm2.5'
FROM 
    (
        SELECT sensor_id, location, lat, lon, timestamp, P1 as pm10, P2 as pm25  FROM ld_all_clean_tbl WHERE lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18
    )
GROUP BY 
    sensor_id,
    lat,
    lon, 
    DATE_TRUNC('hour', timestamp)
ORDER BY 
    sensor_id, 
    DATE_TRUNC('hour', timestamp);

.timer on
SELECT 
    sensor_id,
    lat,
    lon, 
    DATE_TRUNC('hour', timestamp) + INTERVAL 1 HOUR as hour, 
    AVG(pm10) as pm10, 
    AVG(pm25) as 'pm2.5'
FROM (
        SELECT sensor_id, location, lat, lon, timestamp, P1 as pm10, P2 as pm25  FROM read_parquet('data/ld_all_clean_tbl.parquet') WHERE lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18)
GROUP BY 
    sensor_id,
    lat,
    lon, 
    DATE_TRUNC('hour', timestamp)
ORDER BY 
    sensor_id, 
    DATE_TRUNC('hour', timestamp);


SELECT count(*) FROM lep_ld_view;

-- .once -e -- output to text file
.mode json
.mode box
SELECT * FROM lep_ld_view LIMIT 10;
-- next steps: convert to a spatial dataset with duckdb POINT_2d type
.schema