import pandas as pd

# Load the dataset
data_path = '../data/usa_real_estate.csv'
df = pd.read_csv(data_path)

# Display the first few rows of the dataset
print(df.head())

# Display summary statistics
print(df.describe())

# Display data types and non-null counts
print(df.info())

# Check for missing values
missing_values = df.isnull().sum()
print(missing_values[missing_values > 0])

# Handle missing values
df.fillna(df.median(), inplace=True)

# Creating feature 'total_rooms'
df['total_rooms'] = df['bedrooms'] + df['bathrooms']

# One-hot encode categorical variables including 'status'
df = pd.get_dummies(df, columns=['city', 'state', 'status'], drop_first=True)

# Save the cleaned dataset to a new CSV file
df.to_csv('../data/cleaned_usa_real_estate.csv', index=False)
