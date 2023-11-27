import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import csv
from datetime import datetime

# Task 1: Log Progress
def log_progress(stage):
    with open("./code_log.txt", "a") as log_file:
        log_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"{log_time}: {stage}\n")

# Task 2: Extract tabular information
def extract(url):
    log_progress("Extracting data from the URL")

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('table', class_='wikitable')

    #the required table is the first one
    df = pd.read_html(str(tables[0]))[0]
    df.columns = ['Rank', 'Name', 'MC_USD_Billion']
    return df

# Task 3: Transform the dataframe
def transform(df, exchange_rate_file):
    log_progress("Transforming the data")

    exchange_rates = pd.read_csv(exchange_rate_file)
    exchange_rate_dict = exchange_rates.set_index(exchange_rates.columns[0]).to_dict()[exchange_rates.columns[1]]

    df['MC_GBP_Billion'] = [round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    return df

# Task 4: Load to CSV
def load_to_csv(df, output_csv_path):
    log_progress("Loading data to CSV")

    df.to_csv(output_csv_path, index=False)

# Task 5: Load to SQL Database
def load_to_db(df, database_name, table_name):
    log_progress("Loading data to SQL database")

    conn = sqlite3.connect(database_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# Task 6: Run queries on the database
def run_queries(database_name, table_name):
    log_progress("Running queries on the database")

    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Sample queries
    cursor.execute(f"SELECT * FROM {table_name} ")
    print(cursor.fetchall())

    cursor.execute(f"SELECT AVG(MC_GBP_Billion) FROM {table_name} ")
    print(cursor.fetchall())

    cursor.execute(f"SELECT Name FROM {table_name} LIMIT 5")
    print(cursor.fetchall())

    conn.close()

# Task 7: Verify log entries
def verify_log():
    log_progress("Verifying log entries")
    with open("code_log.txt", "r") as log_file:
        print(log_file.read())

# Execute tasks
if __name__ == "__main__":
    # Parameters
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    exchange_rate_csv_path = "./exchange_rate.csv"
    output_csv_path = "./Largest_banks_data.csv"
    database_name = "Banks.db"
    table_name = "Largest_banks"

    # Task 2: Extract
    extracted_df = extract(url)

    # Task 3: Transform
    transformed_df = transform(extracted_df, exchange_rate_csv_path)

    # Task 4: Load to CSV
    load_to_csv(transformed_df, output_csv_path)

    # Task 5: Load to SQL Database
    load_to_db(transformed_df, database_name, table_name)

    # Task 6: Run queries
    run_queries(database_name, table_name)

    # Task 7: Verify log entries
    verify_log()
