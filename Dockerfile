# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
COPY . .

# Set environment variables for Flipkart API credentials
ENV FLIPKART_API_TOKEN=""
ENV FLIPKART_AFFILIATE_ID=""

# Run flipkart_data.py when the container launches
CMD ["python", "flipkart_data.py"]
