import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import numpy as np

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('table', class_='wikitable')
    rows = tables[0].find_all('tr')  # Assuming the table you want is the first one (index 1)

    for row in rows:
        col = row.find_all(['td'])
        if len(col) != 0:
            name = col[1].text.strip()
            mc_usd_billion = col[2].text.strip().replace('\n', '')  # Extracting market cap from the 4th column
            

            data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

# Define the URL and table attribute names
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ["Name", "MC_USD_Billion"]

# Call the function
extracted_data = extract(url, table_attributes)
print(extracted_data)


import pandas as pd

# Read the exchange rate CSV and convert it to a dictionary
file_path = './exchange_rate.csv'
def read_exchange_rate_csv(file_path):
    exchange_df = pd.read_csv(file_path)
    exchange_rate_dict = exchange_df.set_index(exchange_df.columns[0]).to_dict()[exchange_df.columns[1]]
    return exchange_rate_dict

# Transformation function to add new columns based on exchange rates
def transform(df, exchange_rate_dict):
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    return df

extracted_data = extract(url, table_attributes)
df=extracted_data

# Replace 'exchange_rates.csv' with your actual file path
exchange_rate_file_path = './exchange_rate.csv'
exchange_rate = read_exchange_rate_csv(exchange_rate_file_path)

# Transform the DataFrame
transformed_df = transform(df, exchange_rate)

# Print the contents of the transformed DataFrame
print(transformed_df)



