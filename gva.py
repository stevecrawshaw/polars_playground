# %%
import polars as pl
import polars.selectors as cs

# %%
# Read the data
gva: pl.DataFrame = pl.read_csv("data/gva_per_hour_cp_unsmoothed.csv", skip_rows=4)
gva.head()

# %%
# Create a cleaned dataset with only Pounds and area columns
# Using the polars selectors to select columns that start with "Pounds" and "area"
gva_clean: pl.DataFrame = gva.select(cs.starts_with("Pounds"), cs.starts_with("area"))
# %%
# Get the UKX row values for Pounds columns and assign to a list
# Using a list comprehension to extract the values
# Use the polars row method to get the row where area_code is UKX
ukx: list[float] = [
    v
    for k, v in (
        gva_clean.row(by_predicate=pl.col("area_code") == "UKX", named=True).items()
    )
    if k.startswith("Pounds")
]
# %%
ukx

# %%

# Drop area_code and area_name columns to get only Pounds columns in a new dataframe
gva_pounds: pl.DataFrame = gva_clean.drop(pl.col(["area_code", "area_name"]))
# %%
# Use a loop to normalise each Pounds column by the UKX value for that column
# hint: use enumerate to get the index and column name

gva_pounds_normalised_ukx: pl.DataFrame = pl.DataFrame()
for i, col in enumerate(gva_pounds.columns):
    gva_pounds_normalised_ukx = gva_pounds.with_columns(
        (pl.col(col) / ukx[i]).alias(col)
    )

# %%
gva_pounds_normalised_ukx.glimpse()

# %%
# Reconstruct the original dataframe with area_code and area_name columns
# Hint: use pl.concat with how="horizontal"
gva_reconstructed: pl.DataFrame = pl.concat(
    [gva_clean.select(pl.col(["area_code", "area_name"])), gva_pounds_normalised_ukx],
    how="horizontal",
)

# %%

gva_reconstructed.glimpse()
# %%
gva_reconstructed.write_csv("data/gva_per_hour_cp_normalised_ukx.csv")

# %%


# %%


# %%


# %%
# %%


# %%


# %%


# %%


# %%
# %%


# %%


# %%


# %%


# %%
# %%


# %%


# %%


# %%


# %%
