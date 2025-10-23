## Task: Normalize GVA Data by UKX Values

## Description:
You are provided with a dataset containing Gross Value Added (GVA) per hour data for various regions in the UK. The goal is to normalize this data by the UKX (UK average) values. The dataset is in a CSV file named "gva_per_hour_cp_unsmoothed.csv", which includes columns for area codes, area names, and various GVA values in Pounds.

## Tools:
- Polars library for data manipulation in Python.
- Polars selectors for efficient column selection.

## Instructions:

- import polars and polars selectors and alias them
- Read the data
- Create a cleaned dataset with only Pounds and area columns
- Using the polars selectors to select columns that start with "Pounds" and "area"
- Get the UKX row values for Pounds columns and assign to a list
-   Hint: use the polars row function to get values for the UKX row
- Using a list comprehension to extract the values
- Use the polars row method to get the row where area_code is UKX
- Drop area_code and area_name columns to get only Pounds columns in a new dataframe
- Use a loop to normalise each Pounds column by the UKX value for that column
-   hint: use enumerate to get the index and column name
- Reconstruct the original dataframe with area_code and area_name columns
-   Hint: use pl.concat with how="horizontal"
- Write the reconstructed dataframe to a new CSV file