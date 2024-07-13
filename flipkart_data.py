import concurrent.futures
import csv
import io
import os
import zipfile

import requests

# Define the categories to include
included_categories = [
    'fragrances', 'mens_clothing', 'kids_clothing',
    'kids_footwear', 'womens_clothing', 'luggage_travel', 'wearable_smart_devices', 'grooming_beauty_wellness',
    'kitchen_appliances',
    'toys', 'home_appliances', 'home_furnishing', 'womens_footwear'
]
# included_categories = ['fragrances']
# Define the columns to pick
columns_to_pick = [
    'title', 'mrp', 'sellingPrice', 'specialPrice', 'productUrl', 'productBrand',
    'inStock', 'codAvailable', 'offers', 'discount', 'shippingCharges'
]

api_token = '98c21807f30c4c2a818ab339faea0f66'
affiliate_id = 'naninanin'
root_directory = 'product_feeds'  # Root directory to save all CSV files
max_workers = 5  # Number of threads for concurrent processing

url = f"https://affiliate-api.flipkart.net/affiliate/download/feeds/{affiliate_id}.json"

headers = {
    "Fk-Affiliate-Id": affiliate_id,
    "Fk-Affiliate-Token": api_token
}

session = requests.Session()  # Create a session for making requests


def generate_data():
    import pandas as pd

    def process_category(category_name, category_url):
        response2 = session.get(category_url, headers=headers)
        if response2.status_code == 200:
            zip_content = io.BytesIO(response2.content)

            # Extract CSV data from the zip file and filter columns
            with zipfile.ZipFile(zip_content) as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith('.csv'):
                        with zip_file.open(file_name) as csv_file:
                            # Read CSV data and filter columns
                            csv_reader = csv.DictReader(io.TextIOWrapper(csv_file, encoding='utf-8'))
                            filtered_rows = [{col: row[col] for col in columns_to_pick} for row in csv_reader if
                                             row['inStock'] == 'true']

                            # Write filtered data to a Parquet file
                            parquet_output_path = os.path.join(root_directory, f"{category_name}.parquet")
                            df = pd.DataFrame(filtered_rows)
                            df.to_parquet(parquet_output_path, engine='pyarrow')

    # Create the root directory if it doesn't exist
    if not os.path.exists(root_directory):
        os.makedirs(root_directory)
    # Send GET request with headers and parameters
    response = session.get(url, headers=headers)
    # Check response status code
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
    # Get categories
    categories = data["apiGroups"]["affiliate"]["apiListings"].items()
    # Filter categories to include only the specified ones
    filtered_categories = [(category, details) for category, details in categories if category in included_categories]
    # Process categories concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for category, details in filtered_categories:
            category_name = details["apiName"]
            category_url = details["availableVariants"]["v1.1.0"]["get"]
            executor.submit(process_category, category_name, category_url)
    print("All CSV data filtered and saved in the 'product_feeds' directory")


if __name__ == '__main__':
    generate_data()
