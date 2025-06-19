# %%
import altair as alt
import polars as pl

# %%
aq_url = r"https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/air-quality-measurements/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=false&delimiter=%2C"
# %%
aq_tbl = pl.read_csv(aq_url, schema_overrides={"site_id": pl.Utf8})

# %%

aq_tbl.glimpse()

# %%
plot_tbl = aq_tbl.filter(pl.col("site_id") == "215").select(
    pl.col(["year", "annual_mean_no2"])
)
# %%
# using hvplot
chart = plot_tbl.hvplot.points(
    x="year",
    y="annual_mean_no2",
    title="Annual Mean NO2",
)
# %%
chart
# %%
(
    alt.Chart(plot_tbl)
    .mark_point(tooltip=True)
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
