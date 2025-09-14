import json
from kafka import KafkaConsumer

# Create a KafkaConsumer instance
# It will read from the 'messages' topic.
# auto_offset_reset='earliest' means it will read all historical messages.
# group_id ensures that messages are distributed among consumers in the same group.
consumer = KafkaConsumer(
    'test_topics',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='my-message-group'
)

if __name__ == "__main__":
    print("Starting consumer...")
    for message in consumer:
        # The message value is in bytes, so we decode it.
        # Since the producer sent JSON, we load it back into a Python dict.
        print(f"Received message: {json.loads(message.value.decode('utf-8'))}")