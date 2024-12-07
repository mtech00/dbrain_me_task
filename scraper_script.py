import requests
from bs4 import BeautifulSoup
import json
from kafka import KafkaProducer
from datetime import datetime
import time
import os

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# File to store scraped data
json_file = 'scraped_products.json'

# Ensure the JSON file exists
if not os.path.exists(json_file):
    with open(json_file, 'w') as f:
        json.dump([], f)

# Append product data to the JSON file
def append_to_json_file(data):
    with open(json_file, 'r+') as f:
        file_data = json.load(f)
        file_data.append(data)
        f.seek(0)
        json.dump(file_data, f, indent=4)

# Scrape product details
def scrape_product_details(url):
    response = requests.get(url)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    return {
        'name': soup.find('h1', class_='product_title').text.strip(),
        'price': soup.find('p', class_='price').text.strip(),
        'description': soup.find('div', class_='woocommerce-product-details__short-description').text.strip(),
        'stock_number': soup.find('p', class_='stock in-stock').text.strip() if soup.find('p', class_='stock in-stock') else "Out of Stock",
        'url': url
    }

# Scrape main page and send data to Kafka + save to JSON file
def scrape_main_page():
    page = 1
    while True:  # Infinite loop to keep scraping continuously
        response = requests.get(f'https://scrapeme.live/shop/page/{page}/')
        
        if response.status_code != 200:
            print(f"Failed to retrieve page {page}: {response.status_code}")
            time.sleep(10)  # Wait before retrying
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.find_all('li', class_='product')

        # If no products are found on the page, loop back to the first page
        if not products:
            print(f"No products found on page {page}. Returning to page 1.")
            page = 1  # Reset to the first page
            time.sleep(10)  # Short sleep before restarting
            continue

        # Process all products on the current page
        for product in products:
            details = scrape_product_details(product.find('a')['href'])
            if details:
                details['timestamp'] = datetime.now().isoformat()

                # Send to Kafka
                producer.send('products_topic', details)

                # Save to JSON file
                append_to_json_file(details)

                print(f"Sent and saved product: {details['name']}")
                time.sleep(1)  # Delay to avoid overwhelming the server

        page += 1  # Move to the next page

# Run the scraper continuously
try:
    while True:
        scrape_main_page()
        time.sleep(10)  # Optional: Add a delay before restarting the scraping cycle
finally:
    producer.flush()
    producer.close()
