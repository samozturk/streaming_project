from confluent_kafka import Consumer, KafkaError
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'csvdatatopic'
group_id = 'csv_data_consumer_group'

# Create Kafka consumer
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
}

consumer = Consumer(consumer_config)

# Subscribe to the topic
consumer.subscribe([topic_name])

try:
    while True:
        msg = consumer.poll(1.0)  # Wait for message or event/error

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Error: {msg.error()}")
        else:
            # Parse the message value as JSON
            try:
                record = json.loads(msg.value().decode('utf-8'))
                print(f"Received message: {record}")
            except json.JSONDecodeError:
                print(f"Failed to parse message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Interrupted by user, shutting down...")

finally:
    # Close down consumer to commit final offsets.
    consumer.close()