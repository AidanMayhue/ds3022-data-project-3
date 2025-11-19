import websocket
import json
import os
import signal
import sys
from datetime import datetime
from quixstreams import Application
from quixstreams.models import TopicConfig

# ----------------------------
# Kafka Configuration
# ----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "linger.ms": 5,
        "batch.size": 100000,
    }
)

topic = app.topic(
    name="bluesky-events",
    value_serializer="json",
    config=TopicConfig(
        num_partitions=3,
        replication_factor=3
    )
)

producer = app.get_producer()
message_count = 0
start_time = datetime.now()

# ----------------------------
# Graceful shutdown
# ----------------------------
def shutdown(signum, frame):
    print("\nShutting down producer...")
    producer.flush()
    print(f"Total messages produced: {message_count}")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ----------------------------
# WebSocket callbacks
# ----------------------------
def on_message(ws, message):
    global message_count
    try:
        data = json.loads(message)
        rev = data['commit']['rev']
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Skipping invalid message: {e}")
        return

    serialized = topic.serialize(key=rev, value=data)

    try:
        producer.produce(
            topic=topic.name,
            key=serialized.key,
            value=serialized.value
        )
        message_count += 1
    except Exception as e:
        print(f"Error producing message to Kafka: {e}")
        return

    # Print rate every 100 messages
    if message_count % 100 == 0:
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = message_count / elapsed if elapsed > 0 else 0
        print(f"Rate: {rate:.2f} msg/sec (total: {message_count})")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed. Flushing producer...")
    producer.flush()

# ----------------------------
# Start WebSocket
# ----------------------------
ws = websocket.WebSocketApp(
    "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    on_message=on_message,
    on_close=on_close
)

try:
    ws.run_forever()
finally:
    producer.flush()
    print(f"\nTotal messages produced: {message_count}")