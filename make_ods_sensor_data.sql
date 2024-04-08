duckdb data/ld_clean.duckdb
.tables
-- export the sensor location data to a csv file
COPY (SELECT sensor_id, lat as latitude, lon as longitude FROM woe_day_view GROUP BY ALL) TO 'data/dim_woe_sensor.csv' (FORMAT csv);
-- export the sensor data to a csv file
COPY (SELECT sensor_id, date as timestamp, daily_pm10 as pm10, daily_pm25 as pm2_5 FROM woe_day_view) TO 'data/fact_woe_sensor.csv' (FORMAT csv);

.exit