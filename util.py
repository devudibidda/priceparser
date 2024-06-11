import os

import dask.dataframe as dd

# Directory containing CSV files
csv_directory = 'product_feeds'

# Dictionary to store products with more than 50% discount by category
discounted_products_by_category = {}

# Iterate through CSV files in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        category = os.path.splitext(filename)[0]  # Extract category from filename
        df = dd.read_csv(os.path.join(csv_directory, filename), dtype={'discount': 'float64'})
        df['discount_percentage'] = ((df['mrp'] - df['sellingPrice']) / df['mrp']) * 100
        discounted_products = df[df['discount_percentage'] > 30].compute()
        if not discounted_products.empty:
            discounted_products_by_category[category] = discounted_products

# Display the table of products with more than 50% discount for each category
for category, products_df in discounted_products_by_category.items():
    print(f"Category: {category}")
    print(products_df)
    print()
