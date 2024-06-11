import pandas as pd
from amazon_paapi import AmazonApi

# Amazon API credentials
key = 'AKIAIOUEAFM4OTU2RPUQ'
secret = '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'
tag = 'naninaveennet-21'
country = 'IN'

# Category and keywords for search
category = 'Electronics'  # or 'Books', 'Clothing', etc.
keywords = 'discounted electronics'  # or 'discounted books', etc.

# Initialize Amazon API
amazon = AmazonApi(key, secret, tag, country)

# Search for products in the category
response = amazon.search_items(keywords=keywords, search_index=category)

# Retrieve product information and prices
products = []
for item in response.items:
    try:
        product_id = item.asin
        title = item.item_info.title.display_value
        image_url = item.images.primary.medium.url
        offers = item.offers.listings[0]
        price = offers.price.amount
        saving_basis = offers.saving_basis.amount if offers.saving_basis else 0
        discount = saving_basis
        products.append({
            'product_id': product_id,
            'title': title,
            'image_url': image_url,
            'price': price,
            'discount': discount
        })
    except (AttributeError, IndexError):
        # Skip items with missing data
        continue

# Create a Pandas DataFrame to manipulate the data
df = pd.DataFrame(products)

# Calculate the discount percentage
df['discount_percentage'] = (df['discount'] / df['price']) * 100

# Sort the products by discount percentage in descending order
df_sorted = df.sort_values(by='discount_percentage', ascending=False)

# Print the top 10 discounted products
print("Top 10 Discounted Products:")
print(df_sorted.head(10)[['title', 'price', 'discount', 'discount_percentage']])
s
