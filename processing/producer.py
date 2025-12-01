# Imports for WebSocket client, JSON parsing, environment vars, and timestamps
import websocket
import json
import os
from datetime import datetime

# Quix Streams app and topic configuration for Kafka
from quixstreams import Application
from quixstreams.models import TopicConfig

# Kafka broker addresses (default to local cluster if env var not set)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# Initialize Quix Streams application with producer config for batching
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "linger.ms": 5,       # small delay to allow batching
        "batch.size": 100000, # max batch size for Kafka producer
    }
)

# Define Kafka topic with JSON serializer and replication/partition settings
topic = app.topic(
    name="bluesky-events",
    value_serializer="json",
    config=TopicConfig(num_partitions=3, replication_factor=3)
)

# Create a Kafka producer instance
producer = app.get_producer()
message_count = 0  # track how many events have been produced
start_time = datetime.now()  # used to calculate processing rate

# Callback: runs every time a WebSocket message is received
def on_message(ws, message):
    global message_count
    try:
        data = json.loads(message)  # parse raw JSON from the WebSocket stream
        rev = data.get("commit", {}).get("rev")  # use commit revision as Kafka key
        serialized = topic.serialize(key=rev, value=data)  # serialize into Kafka-ready bytes
        producer.produce(topic=topic.name, key=serialized.key, value=serialized.value)  # send to Kafka

        message_count += 1

        # Every 100 messages, print throughput rate
        if message_count % 100 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > 0:
                print(f"Rate: {message_count/elapsed:.2f} msg/sec (total: {message_count})")
    except Exception as e:
        print(f"Error processing message: {e}")

# Callback: runs when WebSocket connection closes
def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed, flushing remaining messages...")
    try:
        producer.flush()  # ensure all buffered messages are delivered
    except Exception as e:
        print(f"Error flushing producer: {e}")

# Create WebSocket client pointing at Bluesky Jetstream
ws = websocket.WebSocketApp(
    "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    on_message=on_message,
    on_close=on_close
)

# Run WebSocket loop until interrupted
try:
    ws.run_forever()
finally:
    # Flush on shutdown for clean exit
    try:
        producer.flush()
    except Exception as e:
        print(f"Error flushing producer on exit: {e}")
    print(f"\nTotal messages produced: {message_count}")
