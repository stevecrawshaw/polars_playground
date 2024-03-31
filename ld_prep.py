# %%

import polars as pl
import duckdb

# %%
path = 'data/2024-01_sds011.csv'


test_df = pl.scan_csv(path,
            null_values = [' ', 'unavailable', 'unknown', 'b'],
            # columns = ['sensor_id', 'location', 'lat', 'lon', 'timestamp', 'P1', 'P2'],
            separator=';',
            ignore_errors = True,
            schema = {
                            'sensor_id': pl.Int64,
                            'sensor_type': pl.Utf8,
                            'location': pl.Utf8,
                            'lat':pl.Float64,
                            'lon':pl.Float64,
                            'timestamp':pl.Datetime,
                            'P1':pl.Float64,
                            'durP1': pl.Float64,
                            'ratioP1': pl.Float64,
                            'P2':pl.Float64,
                            'durP2': pl.Float64,
                            'ratioP2': pl.Float64,
                        },
            try_parse_dates = True,
            n_rows=1000000
            )
# %%


tdf = test_df.collect()
# tdf.glimpse()


# %%
tdf.write_csv('data/tdf100k.csv')
# %%
