import pandas as pd
import os

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

# Load the dataset in chunks and process each chunk
data_path = './data/usa_real_estate.csv'
chunk_size = 50000  # Adjust the chunk size based on your memory capacity

output_file = './data/cleaned_usa_real_estate.csv'
temp_files = []

# Process and save each chunk
for i, chunk in enumerate(pd.read_csv(data_path, dtype=dtype, chunksize=chunk_size)):
    processed_chunk = process_chunk(chunk)
    temp_file = f'./data/temp_cleaned_chunk_{i}.csv'
    processed_chunk.to_csv(temp_file, index=False)
    temp_files.append(temp_file)

# Combine all processed chunks incrementally
header = True
with open(output_file, 'w') as f_out:
    for temp_file in temp_files:
        with open(temp_file, 'r') as f_in:
            if header:
                # Write the header for the first chunk
                f_out.write(f_in.read())
                header = False
            else:
                # Skip the header for subsequent chunks
                next(f_in)
                f_out.write(f_in.read())

# Optionally, clean up temporary files
for temp_file in temp_files:
    os.remove(temp_file)
