import concurrent.futures
import csv
import io
import os
import zipfile
import json
import logging

import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

included_categories = config['included_categories']
columns_to_pick = config['columns_to_pick']

api_token = os.environ.get('FLIPKART_API_TOKEN')
affiliate_id = os.environ.get('FLIPKART_AFFILIATE_ID')
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

    if not api_token or not affiliate_id:
        logging.error("Error: FLIPKART_API_TOKEN and FLIPKART_AFFILIATE_ID environment variables must be set.")
        return

    def process_category(category_name, category_url):
        try:
            response2 = session.get(category_url, headers=headers)
            response2.raise_for_status()  # Raise an exception for bad status codes
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
                            logging.info(f"Successfully processed and saved data for category: {category_name}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for category {category_name}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing category {category_name}: {e}")

    # Create the root directory if it doesn't exist
    if not os.path.exists(root_directory):
        os.makedirs(root_directory)

    try:
        # Send GET request with headers and parameters
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return

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
    logging.info("All CSV data filtered and saved in the 'product_feeds' directory")


if __name__ == '__main__':
    generate_data()
