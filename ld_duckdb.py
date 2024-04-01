# %%
import duckdb
import polars as pl


# %%

con = duckdb.connect('ld.duckdb')
# %%
con.sql("""
SELECT * FROM lep_ld_view
        """).to_parquet('data/lep_ld_view.parquet')
# %%
con.view('ld_all_clean_tbl')
# %%
tables = con.execute('SHOW TABLES;')
print(tables)

# %%
con.close()