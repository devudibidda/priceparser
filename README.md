# Flipkart Discount Finder

This is a web application that helps you find products with high discounts on Flipkart.

## Features

- **Find discounted products:** The application allows you to find products with a discount of over 50%.
- **Browse by category:** You can browse products by category and find the best deals in each category.
- **Easy to use:** The application has a simple and intuitive interface that makes it easy to find what you're looking for.

## Getting Started

### Prerequisites

- Python 3.6 or higher
- Pip

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/your-repository.git
   ```
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```
3. Set the following environment variables:
   ```
   export FLIPKART_API_TOKEN="your_api_token"
   export FLIPKART_AFFILIATE_ID="your_affiliate_id"
   ```

## Usage with Docker

1. **Build the Docker image:**
   ```
   docker build -t flipkart-discount-finder .
   ```
2. **Run the Docker container:**
   ```
   docker run -e FLIPKART_API_TOKEN="your_api_token" -e FLIPKART_AFFILIATE_ID="your_affiliate_id" flipkart-discount-finder
   ```
   This will run the `flipkart_data.py` script and save the product data in the `product_feeds` directory.

3. **Run the web application:**
   To run the web application, you'll need to modify the `Dockerfile` to run `app.py` instead of `flipkart_data.py`. Change the last line of the `Dockerfile` to:
   ```
   CMD ["python", "app.py"]
   ```
   Then, build the image again and run the container with port mapping:
   ```
   docker build -t flipkart-discount-finder-app .
   docker run -p 5000:5000 flipkart-discount-finder-app
   ```
   Now, you can open your web browser and go to `http://localhost:5000` to access the application.
