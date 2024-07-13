import os
import dask.dataframe as dd

# Directory containing CSV files
csv_directory = 'product_feeds'
parquet_directory = 'product_feeds_parquet'

# Convert CSV files to Parquet
if not os.path.exists(parquet_directory):
    os.makedirs(parquet_directory)

for entry in os.scandir(csv_directory):
    if entry.name.endswith('.csv') and entry.is_file():
        df = dd.read_csv(entry.path, dtype={'discount': 'float64'})
        parquet_path = os.path.join(parquet_directory, os.path.splitext(entry.name)[0] + '.parquet')
        df.to_parquet(parquet_path, engine='pyarrow')

# List to store computations
computations = []

# Iterate through Parquet files in the directory
for entry in os.scandir(parquet_directory):
    if entry.name.endswith('.parquet') and entry.is_file():
        category = os.path.splitext(entry.name)[0]  # Extract category from filename
        df = dd.read_parquet(entry.path, engine='pyarrow')
        df['discount_percentage'] = ((df['mrp'] - df['sellingPrice']) / df['mrp']) * 100
        discounted_products = df[df['discount_percentage'] > 10]
        computations.append((category, discounted_products))

# Dictionary to store products with more than 50% discount by category
discounted_products_by_category = {}

# Compute results
for category, computation in computations:
    result = computation.compute()
    print(result)
    if not result.empty:
        discounted_products_by_category[category] = result

# Display the table of products with more than 50% discount for each category
for category, products_df in discounted_products_by_category.items():
    print(f"Category: {category}")
    print(products_df)
    print()
