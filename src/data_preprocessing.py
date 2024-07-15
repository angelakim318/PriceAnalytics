import dask.dataframe as dd
import pandas as pd

# Specify data types explicitly for all columns
dtype = {
    'brokered_by': 'float64',
    'status': 'object',
    'price': 'float64',
    'bed': 'float64',
    'bath': 'float64',
    'acre_lot': 'float64',
    'street': 'object',
    'city': 'object',
    'state': 'object',
    'zip_code': 'float64',
    'house_size': 'float64',
    'prev_sold_date': 'object'
}

# Function to process each chunk
def process_chunk(chunk):
    # Handle missing values for numeric columns
    numeric_cols = chunk.select_dtypes(include=['float64', 'int64']).columns
    chunk[numeric_cols] = chunk[numeric_cols].fillna(chunk[numeric_cols].median())
    
    # Handle missing values for non-numeric columns
    non_numeric_cols = chunk.select_dtypes(exclude=['float64', 'int64']).columns
    for col in non_numeric_cols:
        chunk[col] = chunk[col].fillna(chunk[col].mode().iloc[0])
    
    # Creating feature 'total_rooms'
    chunk['total_rooms'] = chunk['bed'] + chunk['bath']
    
    # One-hot encode categorical variables including 'status'
    chunk = pd.get_dummies(chunk, columns=['city', 'state', 'status'], drop_first=True)
    
    return chunk

# Load the dataset in chunks
data_path = './data/usa_real_estate.csv'
chunk_size = 100000  # Adjust the chunk size based on your memory capacity
chunks = []

for chunk in pd.read_csv(data_path, dtype=dtype, chunksize=chunk_size):
    processed_chunk = process_chunk(chunk)
    chunks.append(processed_chunk)

# Concatenate all processed chunks
df = pd.concat(chunks, ignore_index=True)

# Save the cleaned dataset to a new CSV file
df.to_csv('./data/cleaned_usa_real_estate.csv', index=False)
