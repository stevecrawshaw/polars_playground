# %%
import altair as alt
import polars as pl

# %%
# the URL of a dataset as CSV
aq_url = r"https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/air-quality-measurements/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=false&delimiter=%2C"
# %%
# read the data in as a Polars DataFrame
aq_tbl = pl.read_csv(aq_url, schema_overrides={"site_id": pl.Utf8})

# %%
# have a look at it
aq_tbl.glimpse()


# %%

# group by year, get the mean annual no2 of all sites for each year

annual_mean_no2 = aq_tbl.group_by(pl.col("year")).agg(pl.col("annual_mean_no2").mean())
annual_mean_no2


# %%
# %%
plot_tbl = aq_tbl.filter(pl.col("site_id") == "215").select(
    pl.col(["year", "annual_mean_no2"])
)
# %%
(
    alt.Chart(plot_tbl)
    .mark_line(tooltip=True)
    .encode(
        x=alt.X("year").axis(ticks=False, format="d").scale(domain=[2018, 2025]),
        y=alt.Y("annual_mean_no2").scale(zero=False).title("µgm-3"),
        tooltip=["year", "annual_mean_no2"],
    )
    .configure_scale(
        zero=False,
    )
    .properties(width=600, title="Annual Mean NO2")
    .interactive()
)
# %%

# %%

# %%

test_tbl = aq_tbl.with_columns(pl.lit(None, dtype=pl.Null).alias("new_col"))

# %%
test_tbl.glimpse()


# %%
# Identify columns where all values are null
cols_to_drop = [col.name for col in test_tbl if col.is_null().all()]
# %%
# Drop the identified columns
df_cleaned = test_tbl.drop(cols_to_drop)

print("\nColumns to drop:", cols_to_drop)
print("\nCleaned DataFrame:")
print(df_cleaned)

# %%
