# %%
# import asyncio
import zipfile
from pathlib import Path

import httpx
import ibis

# %%
yr = range(2023, 2024)
mo = range(1, 3)
# ds18b20
urls = [
    f"https://archive.sensor.community/csv_per_month/{y:04d}-{m:02d}/{y:04d}-{m:02d}_sds011.zip"
    for y in yr
    for m in mo
    if not (y == 2024 and m > 4)
]
print(urls)
# %%
con = ibis.connect("duckdb://")


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
            ibtbl.temperature,
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
        # .agg(temp_mean=ibtbl.temperature.mean())
    )


# %%
# async def fetch_data(url):
#     fname = Path(url).name
#     fullpath = Path("/tmp") / fname
#     csvpath = Path("/tmp") / fname.replace(".zip", ".csv")
#     async with httpx.AsyncClient() as client:
#         response = await client.get(url)
#         with open(fullpath, "wb") as f:
#             f.write(response.content)
#         with zipfile.ZipFile(fullpath, "r") as zip_ref:
#             zip_ref.extractall("/tmp")
#         con.read_csv(
#             csvpath,
#             header=True,
#             types={"P1": "DOUBLE", "P2": "DOUBLE"},
#             ignore_errors=True,
#         ).pipe(filt).to_parquet(f"/tmp/{fname.replace('.zip', '.parquet')}")

#         print(f"Downloaded {fname} to {fullpath}")
#         fullpath.unlink()
#         csvpath.unlink()
#         return response.status_code

# %%


def fetch_data_httpx(url):
    fname = Path(url).name
    fullpath = Path("/tmp") / fname
    csvpath = Path("/tmp") / fname.replace(".zip", ".csv")
    response = httpx.get(url)
    with open(fullpath, "wb") as f:
        f.write(response.content)
    with zipfile.ZipFile(fullpath, "r") as zip_ref:
        zip_ref.extractall("/tmp")
    con.read_csv(
        csvpath,
        header=True,
        types={"P1": "DOUBLE", "P2": "DOUBLE"},
        ignore_errors=True,
    ).pipe(filt).to_parquet(f"/tmp/{fname.replace('.zip', '.parquet')}")

    print(f"Downloaded {fname} to {fullpath}")
    fullpath.unlink()
    csvpath.unlink()
    return response.status_code


# %%


# async def main():
#     tasks = [fetch_data(url) for url in urls]
#     responses = await asyncio.gather(*tasks)


#     for res in responses:
#         print(res)
def main():
    [fetch_data_httpx(url) for url in urls]


# %%
if __name__ == "__main__":
    # asyncio.run(main())
    main()
# %%
# ibis.read_parquet("/tmp/2023-01_ds18b20.parquet").to_polars().glimpse()

# %%
