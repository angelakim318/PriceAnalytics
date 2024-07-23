import time
import pandas as pd
from sqlalchemy import create_engine

start_time = time.time()

# Database connection
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
ENDPOINT = 'localhost'
USER = 'angelakim'
PASSWORD = 'angelakim123'  
PORT = 5432
DATABASE = 'house_price_prediction'

# Create database connection
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

# Load the datasets
load_start_time = time.time()
home_values = pd.read_csv('./data/zillow_home_values.csv')
forsale = pd.read_csv('./data/zillow_forsale.csv')
sales = pd.read_csv('./data/zillow_sales.csv')
print(f"Datasets loaded in {time.time() - load_start_time} seconds")

# Convert Date columns to datetime format
home_values.columns = home_values.columns.str.replace('Unnamed: 0', 'RegionID')
forsale.columns = forsale.columns.str.replace('Unnamed: 0', 'RegionID')
sales.columns = sales.columns.str.replace('Unnamed: 0', 'RegionID')

# Melt the datasets to long format
melt_start_time = time.time()
home_values_melted = home_values.melt(id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'State', 'City', 'Metro', 'CountyName'], 
                                      var_name='Date', value_name='HomeValue')
forsale_melted = forsale.melt(id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'], 
                              var_name='Date', value_name='ListPrice')
sales_melted = sales.melt(id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'], 
                          var_name='Date', value_name='SalePrice')
print(f"Datasets melted in {time.time() - melt_start_time} seconds")

# Convert Date column to datetime format
home_values_melted['Date'] = pd.to_datetime(home_values_melted['Date'])
forsale_melted['Date'] = pd.to_datetime(forsale_melted['Date'])
sales_melted['Date'] = pd.to_datetime(sales_melted['Date'])

# Convert key columns to consistent data types
key_columns = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
for col in key_columns:
    home_values_melted[col] = home_values_melted[col].astype(str)
    forsale_melted[col] = forsale_melted[col].astype(str)
    sales_melted[col] = sales_melted[col].astype(str)

# Merge the datasets on common columns
merge_start_time = time.time()
combined_data = pd.merge(home_values_melted, forsale_melted, 
                        on=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'Date'], 
                        how='left')
combined_data = pd.merge(combined_data, sales_melted, 
                        on=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName', 'Date'], 
                        how='left')
print(f"Datasets merged in {time.time() - merge_start_time} seconds")

# Fill NaN values if necessary
combined_data.fillna(0, inplace=True)

# Insert combined data into PostgreSQL
insert_start_time = time.time()
combined_data.to_sql('combined_real_estate', engine, if_exists='append', index=False)
print(f"Combined data inserted into PostgreSQL in {time.time() - insert_start_time} seconds")

total_time = time.time() - start_time
print(f"Total time taken: {total_time} seconds")
