# %%

import zipfile
import duckdb
import os

# %%

def extract_zip_files(folder_path):


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
                COPY SELECT * FROM read_csv('{csv_path}',
                delim = ";",
                header = true,
                {col_dict},               
                ignore_errors = true) TO '{parquet_path}' (FORMAT PARQUET);
                """

            duckdb.query(qry)
            # os.remove(zip_path)

# %%
# Extract all the .zip files in the specified folder
extract_zip_files('data')

# %%
