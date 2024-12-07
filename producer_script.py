from kafka import KafkaProducer

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send a message to the 'hello_kafka' topic
producer.send('hello_kafka', b'Hello, Kafka!')

# Ensure all messages are sent before exiting
producer.flush()

print("Message sent to 'hello_kafka:'")
