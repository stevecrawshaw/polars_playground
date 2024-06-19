# %%

import zipfile
import duckdb
import os
import glob
import polars as pl

# %%

columns =  """ columns ={
                    'sensor_id': BIGINT,
                    'sensor_type': VARCHAR,
                    'location': VARCHAR,
                    'lat': FLOAT,
                    'lon': FLOAT,
                    'timestamp': TIMESTAMP,
                    'pressure': FLOAT,
                    'altitude': FLOAT,
                    'pressure_sealevel': FLOAT,
                    'temperature': FLOAT,
                    'humidity': FLOAT
                }"""    

#%%
def read_csv(csv_file,
        columns, 
        folder_path = 'data',
        delim = ';', 
        header = True, 
        ignore_errors = True):
    csv_path = os.path.join(folder_path, csv_file)
    parquet_path = os.path.join(folder_path, csv_file.replace('.csv', '.parquet'))
    qry = f"""
    COPY (SELECT * FROM read_csv('{csv_path}',
    delim = '{delim}',
    header = {header},
    {columns},               
    ignore_errors = {ignore_errors})) TO '{parquet_path}' (FORMAT PARQUET);
    """
    duckdb.query(qry)
    #print(qry)
    return parquet_path

 # %%


p_file = read_csv('2023-01_bme280.csv', columns = columns)


# %%
woe_bme_qry = f"""
SELECT 
        sensor_id,
        lat,
        lon, 
        DATE_TRUNC('hour', timestamp) + INTERVAL '1 hour' as hour, 
        AVG(pressure) as pressure, 
        AVG(temperature) as temperature,
        AVG(humidity) as humidity
    FROM (
        SELECT 
            sensor_id, lat, lon, timestamp, pressure, temperature, humidity  
        FROM 
            read_parquet('{p_file}')
        WHERE 
            lat >= 50 AND lat <= 53 AND lon >= -4.5 AND lon <= -1.7
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


# %%


df = duckdb.query(woe_bme_qry).pl()
df.describe()

# %%

df.unique('sensor_id')



# %%





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
