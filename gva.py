# %%
import polars as pl
import polars.selectors as cs

# %%
gva = pl.read_csv("data/gva_per_hour_cp_unsmoothed.csv", skip_rows=4)

# %%
gva.head()
# %%
gva.glimpse()

# %%
gva_clean = gva.select(cs.starts_with("Pounds"), cs.starts_with("area"))

# %%
gva_clean.glimpse()

# %%
gva_clean.head()

# %%
ukx = [
    v
    for k, v in (
        gva_clean.row(by_predicate=pl.col("area_code") == "UKX", named=True).items()
    )
    if k.startswith("Pounds")
]
# %%
ukx

# %%


# %%


# %%


# %%


# %%
