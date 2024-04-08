# %%

import zipfile
import duckdb
import os
import glob
import polars as pl
# %%
def extract_zip_files(folder_path):
    """
    Extract all the .zip files in the specified folder.
    The extracted files will be in the same folder as the .zip files.
    Read the CSV files and write them to Parquet files.
    Delete the .zip and .csv files after writing the Parquet files.

    """

    # Iterate through all files in the specified folder
    for filename in os.listdir(folder_path):
        # Check if the file is a .zip file
        if filename.endswith('.zip'):
            # Construct the full path to the .zip file
            zip_path = os.path.join(folder_path, filename)
            
            # Open the .zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract all the contents into the same folder
                csv_file = zip_ref.namelist()[0]
                zip_ref.extractall(folder_path)
                print(f"Extracted {filename}")
                csv_path = os.path.join(folder_path, csv_file)
                parquet_path = os.path.join(folder_path, csv_file.replace('.csv', '.parquet'))
                col_dict =  """columns = {
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
                }"""                
                qry = f"""
                COPY (SELECT * FROM read_csv('{csv_path}',
                delim = ";",
                header = true,
                {col_dict},               
                ignore_errors = true)) TO '{parquet_path}' (FORMAT PARQUET);
                """

            duckdb.query(qry)
            os.remove(zip_path)
            os.remove(csv_path)

# %%
# Extract all the .zip files in the specified folder
if any(filename.endswith('.zip') for filename in os.listdir('data')):
    extract_zip_files('data')

# %%

con = duckdb.connect('data/ld_clean.duckdb')

# %%
con.sql('SHOW TABLES;')
# %%

con.sql('FROM fact_ld_tbl LIMIT 10')

# %%
create_ld_tbl = """
CREATE OR REPLACE TABLE ld_clean_tbl 
(sensor_id BIGINT, lat FLOAT, lon FLOAT, hour TIMESTAMP, pm10 FLOAT, "pm25" FLOAT);
"""
create_dim_ld_tbl = """
CREATE OR REPLACE TABLE dim_ld_tbl 
(sensor_id BIGINT, lat FLOAT, lon FLOAT);
"""
create_fact_ld_tbl = """
CREATE OR REPLACE TABLE fact_ld_tbl 
(sensor_id BIGINT, hour TIMESTAMP, pm10 FLOAT, pm25 FLOAT);
"""

# %%

def insert_data_from_parquet(file):
    """
    Accepts a parquet file and groups by sensor_id and hour (ending) to 
    calculate hourly mean PM10 and PM2.5 for the month
    Inserts into the temporary table ld_clean_tbl
    """
    copy_qry = f"""
    INSERT INTO ld_clean_tbl (sensor_id, lat, lon, hour, pm10, pm25)
    SELECT 
        sensor_id,
        lat,
        lon, 
        DATE_TRUNC('hour', timestamp) + INTERVAL '1 hour' as hour, 
        AVG(pm10) as pm10, 
        AVG(pm25) as pm25
    FROM (
        SELECT 
            sensor_id, lat, lon, timestamp, P1 as pm10, P2 as pm25  
        FROM 
            read_parquet('{file}')
        WHERE 
            lat >= 50 AND lat <= 53 AND lon >= -4.5 AND lon <= 1.7
    ) AS subquery
    GROUP BY 
        sensor_id,
        lat,
        lon, 
        DATE_TRUNC('hour', timestamp)
    ORDER BY 
        sensor_id, 
        DATE_TRUNC('hour', timestamp);
    """
# WoE lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18
# Bounding box in query is for UK
    con.execute(copy_qry)
# %%

def insert_dim():
    copy_qry = """
    INSERT INTO dim_ld_tbl (sensor_id, lat, lon)
    SELECT 
        sensor_id, lat, lon
    FROM (
        SELECT 
            sensor_id, lat, lon
        FROM 
            ld_clean_tbl
    ) AS subquery
    GROUP BY 
        sensor_id, lat, lon
    ORDER BY 
        sensor_id;
    """
    con.execute(copy_qry)

def insert_fact():
    copy_qry = """
    INSERT INTO fact_ld_tbl (sensor_id, hour, pm10, pm25)
    SELECT 
        sensor_id, hour, pm10, pm25
    FROM (
        SELECT 
            sensor_id, hour, pm10, pm25
        FROM 
            ld_clean_tbl
    ) AS subquery
    ORDER BY 
        sensor_id;
    """
    con.execute(copy_qry)

create_woe_hour_view_qry = """
CREATE OR REPLACE VIEW woe_hour_view AS
SELECT 
    fact_ld_tbl.sensor_id, 
    lat, 
    lon, 
    hour, 
    pm10, 
    pm25
FROM 
    fact_ld_tbl
LEFT JOIN
    dim_ld_tbl ON fact_ld_tbl.sensor_id = dim_ld_tbl.sensor_id
WHERE 
    lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18
ORDER BY 
    fact_ld_tbl.sensor_id, 
    hour;
"""
create_daily_view_qry = """
CREATE OR REPLACE VIEW daily_view AS
SELECT 
    fact_ld_tbl.sensor_id,
    date_trunc('day', hour) as date,
    AVG(pm10) as daily_pm10,
    AVG(pm25) as daily_pm25 
FROM fact_ld_tbl 
LEFT JOIN
    dim_ld_tbl ON fact_ld_tbl.sensor_id = dim_ld_tbl.sensor_id
GROUP BY fact_ld_tbl.sensor_id, date
ORDER BY fact_ld_tbl.sensor_id, date;
"""

create_woe_day_view_qry = """
CREATE OR REPLACE VIEW woe_day_view AS
SELECT 
    fact_ld_tbl.sensor_id,
    lat,
    lon,
    date_trunc('day', hour) as date,
    AVG(pm10) as daily_pm10,
    AVG(pm25) as daily_pm25 
FROM fact_ld_tbl 
LEFT JOIN
    dim_ld_tbl ON fact_ld_tbl.sensor_id = dim_ld_tbl.sensor_id
WHERE 
    lat >= 51.2 AND lat <= 51.6 AND lon >= -3.0 AND lon <= -2.18
GROUP BY fact_ld_tbl.sensor_id, lat, lon, date
ORDER BY fact_ld_tbl.sensor_id, date;
"""

# %%

try:
    con.execute("BEGIN TRANSACTION;")
    #con.execute('INSTALL spatial;')
    #con.execute('LOAD spatial;')
    con.sql(create_ld_tbl)
    con.sql(create_dim_ld_tbl)
    con.sql(create_fact_ld_tbl)

    for filename in os.listdir('data'):
        if filename.endswith('.parquet'):
            insert_data_from_parquet(os.path.join('data', filename))
            print(f"Inserted data from {filename}")
            insert_dim()
            insert_fact()
            con.execute("DELETE FROM ld_clean_tbl;")
    con.sql('DELETE FROM fact_ld_tbl WHERE pm10 > 1998;')
    con.sql(create_daily_view_qry)
    con.sql(create_woe_hour_view_qry)
    con.sql(create_woe_day_view_qry)
    con.sql('DROP TABLE ld_clean_tbl;')
    con.execute("COMMIT;")
    con.execute('CHECKPOINT;')
except Exception as e:
    # If an error occurs, rollback the transaction
    con.execute("ROLLBACK;")
    print(f"Transaction rolled back due to an error: {e}")

# %%
con.sql('SHOW TABLES;')
# %%
con.sql('SELECT count(*) FROM fact_ld_tbl')
# %%
con.sql('SELECT * FROM woe_day_view LIMIT 10')
# %%
con.sql('SELECT * FROM fact_ld_tbl WHERE pm10 > 200')
# %%
con.sql('DESCRIBE fact_ld_tbl')
# %%
con.close()




# %%
