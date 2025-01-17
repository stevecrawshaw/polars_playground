#%%
# load polars library
import polars as pl

#%%
# get the postcode lookup data
pc_lu_df = pl.scan_csv("../weca_cesap/data/postcode_lookup/PCD_OA21_LSOA21_MSOA21_LAD_AUG23_UK_LU.csv")



#%%
# inspect for field names
pc_lu_df.head().collect()

#%%
# get the postcode areas for the west of england
woe_lookup_df = (pc_lu_df
                 .filter(pl.col('ladnm').str.contains('Bristol|Bath and North East Somerset|South Gloucestershire'))
                 .select(['pcd7', 'ladnm'])
                 .with_columns(pl.col('pcd7').str.head(4).str.strip_chars_end(" ").alias('pcd4'))
                 .drop(['pcd7', 'ladnm'])
                 .unique()
                 ).collect()

#%%
# create a series to use as a filter in the next stage
pcd4 = woe_lookup_df.select('pcd4').to_series().to_list()
print(pcd4)
#%%

# load the nomis visa data as a lazyframe
visa_nomis_raw_df = pl.scan_csv("../regional_evidence/data/POSTAL_DISTRICT_Quarterly_indexed_map_data.csv")
#%%
# inspect the first few rows
visa_nomis_raw_df.head().collect()


# %%
%%time
# time the operation

# filter the data to only include the west of england postcodes
# and extract the year and quarter from the time_period_value field
ba_bs_df = (visa_nomis_raw_df
            .filter((pl.col('cardholder_location').is_in(pcd4)) | (pl.col('merchant_location').is_in(pcd4)))
            .with_columns([pl.col('time_period_value').str.head(4).cast(pl.Int16).alias('year'),
                           pl.col('time_period_value').str.tail(1).cast(pl.Int8).alias('quarter')])
            .drop('time_period_value')
            ).collect()

# %%
# inspect the first few rows
ba_bs_df.head()

# %%
ba_bs_df.write_csv('../regional_evidence/data/visa_nomis_weca.csv')
# %%
ba_bs_df.write_parquet('../regional_evidence/data/visa_nomis_weca.parquet')