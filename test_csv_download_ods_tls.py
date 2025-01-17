#%%
import polars as pl
import requests

#%%
print(requests.certs.where())
#%%
# Define the URL of the CSV file
url = "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/lep-epc-domestic-point/exports/csv?lang=en&refine=property_type%3A%22Park%20home%22&facet=facet(name%3D%22property_type%22%2C%20disjunctive%3Dtrue)&timezone=Europe%2FLondon&use_labels=true&delimiter=%2C"

#%%
# Send a GET request to the URL
response = requests.get(url)
print(response.request.headers)
#%%
# Check if the request was successful
if response.status_code == 200:
    # Save the content of the request to a file
    with open('file.csv', 'wb') as f:
        f.write(response.content)

    # Read the CSV file into a Polars DataFrame
    df = pl.read_csv('file.csv')

    # Display the DataFrame
    print(df)
else:
    print(f"Failed to download file. Status code: {response.status_code}")

# %%
