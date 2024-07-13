import os
import dask.dataframe as dd
from flask import Flask, render_template, request, redirect, url_for
from dask.distributed import Client
from dask.cache import Cache

app = Flask(__name__)
csv_directory = 'product_feeds'

# Start a Dask client
client = Client(processes=False, dashboard_address=':0')

# Enable caching with 2GB of memory
cache = Cache(2e9)  # 2GB
cache.register()

@app.route('/')
def index():
    categories = [os.path.splitext(entry.name)[0] for entry in os.scandir(csv_directory) if entry.name.endswith('.parquet') and entry.is_file()]
    return render_template('index.html', categories=categories)

@app.route('/calculate_discount', methods=['POST'])
def calculate_discount():
    category = request.form.get('category')
    file_path = os.path.join(csv_directory, f"{category}.parquet")

    if os.path.isfile(file_path):
        df = dd.read_parquet(file_path, engine='pyarrow')

        df['sellingPrice'] = df['sellingPrice'].astype(float)
        df['specialPrice'] = df['specialPrice'].astype(float)

        df['discount_percentage'] = ((df['sellingPrice'] - df['specialPrice']) / df['sellingPrice']) * 100
        discounted_products = df[df['discount_percentage'] > 50]

        discounted_products = client.persist(discounted_products)
        result = discounted_products.compute().sort_values(by='discount_percentage', ascending=False)
        print(result)
        print(f"DataFrame columns: {result.columns}")  # Print DataFrame columns
        print(f"Number of discounted products: {len(result)}")

        if not result.empty:
            return render_template('discount.html', discounted_products=result)

    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=False, port=5000)