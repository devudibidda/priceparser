import os

import dask.dataframe as dd
from flask import Flask, render_template

app = Flask(__name__)
csv_directory = 'product_feeds'


@app.route('/')
def index():
    discounted_products_by_category = {}
    computations = []

    for entry in os.scandir(csv_directory):
        if entry.name.endswith('.csv') and entry.is_file():
            category = os.path.splitext(entry.name)[0]
            df = dd.read_csv(entry.path, dtype={'discount': 'float64'})
            df['discount_percentage'] = ((df['mrp'] - df['sellingPrice']) / df['mrp']) * 100
            discounted_products = df[df['discount_percentage'] > 50]
            computations.append((category, discounted_products))

    for category, computation in computations:
        result = computation.compute()
        if not result.empty:
            discounted_products_by_category[category] = result

    return render_template('index.html', discounted_products_by_category=discounted_products_by_category)


if __name__ == '__main__':
    app.run(debug=True, port=5000)