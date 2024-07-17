import pandas as pd
from sqlalchemy import create_engine

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

# Load CSV in chunks and insert into PostgreSQL
# data_path = './data/usa_real_estate.csv'
# chunksize = 10000

# for chunk in pd.read_csv(data_path, chunksize=chunksize):
#     chunk.to_sql('real_estate', engine, if_exists='append', index=False)

# print("Data loaded into PostgreSQL successfully.")

# Export cleaned data to CSV
cleaned_data = pd.read_sql("SELECT * FROM real_estate", engine)
cleaned_data.to_csv('./data/cleaned_usa_real_estate.csv', index=False)

print("Cleaned data exported to cleaned_usa_real_estate.csv successfully.")