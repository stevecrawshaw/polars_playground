# %%
import polars as pl
import polars.selectors as cs

# %%
# Read the data
gva: pl.DataFrame = pl.read_csv(
    "data/gva_per_hour_cp_unsmoothed.csv", skip_rows=4, n_rows=34
)

# %%
# Create a cleaned dataset with only Pounds and area columns
gva_clean: pl.DataFrame = gva.select(cs.starts_with("Pounds"), cs.starts_with("area"))

# %%
# --- Elegant Normalisation ---

# 1. Get the list of column names we need to normalise
pounds_cols: list[str] = gva_clean.select(cs.starts_with("Pounds")).columns

# 2. Build a list of expressions.
# For each 'Pounds' column, we create an expression that:
#   a. Takes the column (pl.col(col_name))
#   b. Divides it by the value from that same column,
#      but filtered down to the UKX row (.filter(pl.col("area_code") == "UKX").first())
norm_expressions: list[pl.Expr] = [
    (
        pl.col(col_name) / pl.col(col_name).filter(pl.col("area_code") == "UKX").first()
    ).alias(col_name)
    for col_name in pounds_cols
]

# 3. Apply all normalisation expressions in a single, parallel operation.
# This updates the 'Pounds' columns within the 'gva_clean' DataFrame,
# so no reconstruction is needed.
gva_reconstructed: pl.DataFrame = gva_clean.with_columns(norm_expressions)

# --- End of Elegant Normalisation ---

# %%
gva_reconstructed.glimpse()

# %%
gva_reconstructed.write_csv("data/gva_per_hour_cp_normalised_ukx_gemini.csv")
