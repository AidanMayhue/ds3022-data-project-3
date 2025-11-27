import websocket
import json
import os
import time
from datetime import datetime
from quixstreams import Application
from quixstreams.models import TopicConfig

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
)

# Initialize Kafka application
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "linger.ms": 5,       # Batch messages for up to 5ms
        "batch.size": 100000, # Batch up to 100KB
    }
)

# Safe topic creation
try:
    topic = app.topic(
        name="bluesky-events",
        value_serializer="json",
        config=TopicConfig(
            num_partitions=3,
            replication_factor=3
        )
    )
except Exception as e:
    print(f"Warning: Could not create topic (it may already exist): {e}")
    topic = app.topic(name="bluesky-events", value_serializer="json")

# Kafka producer
producer = app.get_producer()

# Counters for metrics
message_count = 0
start_time = datetime.now()

def on_message(ws, message):
    global message_count
    try:
        data = json.loads(message)
        rev = data['commit']['rev']
        serialized = topic.serialize(key=rev, value=data)

        # Produce to Kafka
        producer.produce(topic=topic.name, key=serialized.key, value=serialized.value)
        message_count += 1

        # Print rate every 100 messages
        if message_count % 100 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > 0:
                print(f"Rate: {message_count / elapsed:.2f} msg/sec (total: {message_count})")

    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e} | message skipped")
    except Exception as e:
        print(f"Error producing message: {e}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed, flushing remaining messages...")
    try:
        producer.flush()
    except Exception as e:
        print(f"Error flushing producer: {e}")

def run_websocket():
    ws = websocket.WebSocketApp(
        "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
        on_message=on_message,
        on_close=on_close
    )

    while True:
        try:
            ws.run_forever()
        except Exception as e:
            print(f"WebSocket connection error: {e}, reconnecting in 5 seconds...")
            time.sleep(5)
        else:
            break

if __name__ == "__main__":
    try:
        run_websocket()
    finally:
        # Ensure all messages are flushed on exit
        try:
            producer.flush()
        except Exception as e:
            print(f"Error flushing producer on exit: {e}")
        print(f"\nTotal messages produced: {message_count}")
