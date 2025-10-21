# %%
import polars as pl
import polars.selectors as cs

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
# add a new column with all None values for testing
# The data type is type Utf8 (string)
test_tbl = aq_tbl.with_columns(pl.lit(None, dtype=pl.Utf8).alias("new_col"))
# %%
test_tbl.glimpse()

# new_col is of type Null and has all null values
# %%
# Identify columns where all values are null

# This is a list comprehension that checks each column in the DataFrame.
# It returns (assigns to cols_to_drop) the names of columns where all values are null.

cols_to_drop = [col.name for col in test_tbl if col.is_null().all()]
# %%
# Drop the identified columns
df_cleaned = test_tbl.drop(cols_to_drop)

print("\nColumns to drop:", cols_to_drop)
print("\nCleaned DataFrame:")
df_cleaned.glimpse()
# %%

test_tbl = aq_tbl.with_columns(pl.lit(None, dtype=pl.Null).alias("new_col_type_null"))
# %%
test_tbl.glimpse()
# %%

# using selector to drop all columns of type Null
# This only works if you have a column with dtype Null
# Which would be an unusual case in practice
# But is a good demonstration of using selectors
# - more elegant than a list comprehension
test_tbl.drop(cs.by_dtype(pl.Null)).glimpse()
