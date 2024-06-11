import os

import dask.dataframe as dd
from flask import Flask, render_template

app = Flask(__name__)

# Directory containing CSV files
csv_directory = 'product_feeds'


@app.route('/')
def index():
    # Dictionary to store products with more than 50% discount by category
    discounted_products_by_category = {}

    # Iterate through CSV files in the directory
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            category = os.path.splitext(filename)[0]  # Extract category from filename
            df = dd.read_csv(os.path.join(csv_directory, filename), dtype={'discount': 'float64'})
            df['discount_percentage'] = ((df['mrp'] - df['sellingPrice']) / df['mrp']) * 100
            discounted_products = df[df['discount_percentage'] > 50].compute()
            if not discounted_products.empty:
                discounted_products_by_category[category] = discounted_products

    return render_template('index.html', discounted_products_by_category=discounted_products_by_category)


if __name__ == '__main__':
    app.run(debug=True, port=8080)
