
---

# Kafka Scraper API Project

This project uses **Kafka** and **Docker** to scrape data from a website, send it to a Kafka topic, store it in a JSON file, and serve it via a REST API.

## How to Run the Project

1. Make sure **Docker** is installed on your system.

2. Before running, ensure the following ports are free:
   - **Port 9092** (for Kafka)
   - **Port 5000** (for the Flask API)

3. Build and run the project using these commands:

```bash
docker build -t kafka-scraper-api .
docker run -p 9092:9092 -p 5000:5000 kafka-scraper-api
```

4. After 5-10 seconds, you can access the API at the following URLs:
   - View the latest data: [http://127.0.0.1:5000/get-data](http://127.0.0.1:5000/get-data)
   - Download the data as a JSON file: [http://127.0.0.1:5000/download-data](http://127.0.0.1:5000/download-data)

   **Note**: The JSON file is updated as the scraper runs the data refreshes when the page is reloaded.

## Project Overview

### Task 1: Kafka Setup

- **Kafka in Docker**: Kafka is set up inside a Docker container using the `Dockerfile`.
  
- **Kafka Topic Creation**: A Kafka topic is created using the `producer_script.py` file.

- **Message Sending and Listening**: Messages are sent to and listened from a Kafka topic.
- kafka-console-producer.sh --topic hello_kafka --bootstrap-server localhost:9092
- kafka-console-consumer.sh --topic hello_kafka --from-beginning --bootstrap-server localhost:9092


- **Kafka Configuration**: The `kraft-server.properties` file configures Kafka with the necessary server settings.

### Task 2: Scraping and Kafka Integration

- **Data Scraping**: The `scraper_script.py` script scrapes data from the [https://scrapeme.live/shop/](https://scrapeme.live/shop/) website every 1 second.

  Example code for scraping:
  ```python
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
     ```
   
   - **Send Data to Kafka**: The `producer_script.py` script sends the scraped data to a Kafka topic in JSON format at 1-second intervals.
   
     Example code for sending data to Kafka:
     ```python
     from kafka import KafkaProducer
     import json
   
     producer = KafkaProducer(bootstrap_servers='localhost:9092',
                              value_serializer=lambda v: json.dumps(v).encode('utf-8'))
   
     def send_to_kafka(data):
         producer.send('my_topic', data)
     ```
   
   - **Save Data to File**: The data sent to Kafka is also saved to a local JSON file.
   
   - **REST API**: The `rest_api.py` file serves the data via HTTP using Flask.
   
     Example API routes:
     ```python
     from flask import Flask, jsonify, send_file
     import json
   
     app = Flask(__name__)
   
     @app.route('/get-data', methods=['GET'])
     def get_data():
         with open('data.json', 'r') as file:
             data = json.load(file)
         return jsonify(data)
   
     @app.route('/download-data', methods=['GET'])
     def download_data():
         return send_file('data.json', as_attachment=True)
   
     if __name__ == '__main__':
         app.run(host='0.0.0.0', port=5000)
     ```

### Task 3: Dockerizing the Project

- **Dockerize Everything**: The project is fully dockerized using the `Dockerfile`. Once the Docker image is built and run, all services start automatically.

## Files Included

- **`Dockerfile`**: Contains the Docker setup for the project.
- **`rest_api.py`**: The Flask REST API for accessing and downloading the scraped data.
- **`scraper_script.py`**: The script that scrapes data from the website.
- **`producer_script.py`**: For creating kafka topic
- **`kraft-server.properties`**: Kafka server configuration file.
- **`requirements.txt`**: Lists the required Python packages for the project.



---
