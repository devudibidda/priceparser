import os

import dask.dataframe as dd

# Directory containing CSV files
csv_directory = 'product_feeds'

# List to store computations
computations = []

# Iterate through CSV files in the directory
for entry in os.scandir(csv_directory):
    if entry.name.endswith('.csv') and entry.is_file():
        category = os.path.splitext(entry.name)[0]  # Extract category from filename
        df = dd.read_csv(entry.path, dtype={'discount': 'float64'})
        df['discount_percentage'] = ((df['mrp'] - df['sellingPrice']) / df['mrp']) * 100
        discounted_products = df[df['discount_percentage'] > 30]
        computations.append((category, discounted_products))

# Dictionary to store products with more than 50% discount by category
discounted_products_by_category = {}

# Compute results
for category, computation in computations:
    result = computation.compute()
    if not result.empty:
        discounted_products_by_category[category] = result

# Display the table of products with more than 50% discount for each category
for category, products_df in discounted_products_by_category.items():
    print(f"Category: {category}")
    print(products_df)
    print()