import csv
from confluent_kafka import Producer
import json
import time

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'csvdatatopic'

# Create Kafka producer
producer_config = {
    'bootstrap.servers': bootstrap_servers,
}
producer = Producer(producer_config)

# CSV file path
csv_file_path = '../data/revised.csv'

# Function to delivery report (callback)
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Read CSV and send to Kafka
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Convert row to JSON
        json_data = json.dumps(row)
        
        # Send each row as a message to Kafka
        producer.produce(topic_name, value=json_data, callback=delivery_report)
        print(f"Sent: {row}")
        
        # Optional: add a small delay to simulate streaming
        time.sleep(0.1)
        
        # Serve delivery callback queue
        producer.poll(0)

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()

print("All data has been sent to Kafka")