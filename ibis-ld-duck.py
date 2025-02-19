# %%
import glob

import ibis

# %%
con = ibis.connect("duckdb://data/ld.duckdb")
ld_files = glob.glob("data/2024_data/*.parquet")


# %%
def filt(ibtbl: ibis.table):
    return (
        ibtbl.filter(ibtbl.lat.between(51.3, 51.6), ibtbl.lon.between(-2.9, -2.2))
        .select(
            ibtbl.sensor_id,
            ibtbl.location,
            ibtbl.lat,
            ibtbl.lon,
            ibtbl.timestamp,
            ibtbl.P1,
            ibtbl.P2,
        )
        .order_by(ibtbl.timestamp)
        .group_by(
            [
                ibtbl.timestamp.bucket(hours=1).name("ts_hour"),
                ibtbl.sensor_id,
                ibtbl.location,
                ibtbl.lat,
                ibtbl.lon,
            ]
        )
        .agg(p1_mean=ibtbl.P1.mean(), p2_mean=ibtbl.P2.mean())
    )


# %%
tbl = None
for file in ld_files:
    if tbl is None:
        tbl = con.read_parquet(file).pipe(filt)
    else:
        tbl = tbl.union(con.read_parquet(file).pipe(filt))

# %%

woe_data = tbl.to_polars()

# %%
woe_data.glimpse()
# %%
# %%
tbl.to_parquet("data/woe_data.parquet")
# %%


# %%


# %%


# %%


# %%


# %%
