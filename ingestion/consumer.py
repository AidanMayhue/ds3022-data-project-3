import os
import duckdb
import signal
import sys
from quixstreams import Application

# ----------------------------
# Kafka Configuration
# ----------------------------
KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
)

app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="bluesky-duck-consumer",
    consumer_extra_config={
        "broker.address.family": "v4",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
)

topic = app.topic(name="bluesky-events", value_deserializer="json")
consumer = app.get_consumer()
consumer.subscribe([topic.name])

# ----------------------------
# DuckDB Setup
# ----------------------------
db = duckdb.connect("bluesky.db")
db.execute("""
CREATE TABLE IF NOT EXISTS bluesky_events (
    rev TEXT,
    commit_time TIMESTAMP,
    repo TEXT,
    record JSON
)
""")

# ----------------------------
# Graceful shutdown
# ----------------------------
def shutdown(signum, frame):
    print("\nStopping consumer...")
    consumer.close()
    db.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ----------------------------
# Main loop with batch insert
# ----------------------------
BATCH_SIZE = 50  # adjust based on throughput
batch = []

print("Consumer started. Writing messages into DuckDB...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        try:
            key = topic.deserialize_key(msg.key())
            value = topic.deserialize_value(msg.value())
        except Exception as e:
            print(f"Skipping invalid message: {e}")
            continue

        rev = key
        commit_time = value.get("commit", {}).get("time")
        repo = value.get("commit", {}).get("repo")
        record_json = value

        batch.append((rev, commit_time, repo, record_json))

        # Insert batch
        if len(batch) >= BATCH_SIZE:
            try:
                db.executemany(
                    "INSERT INTO bluesky_events (rev, commit_time, repo, record) VALUES (?, ?, ?, ?)",
                    batch
                )
                consumer.commit(asynchronous=False)
                print(f"Inserted batch of {len(batch)} messages.")
                batch.clear()
            except Exception as e:
                print(f"Error inserting batch: {e}")
                # Do not commit offsets if DB insert fails

except KeyboardInterrupt:
    shutdown(None, None)
finally:
    if batch:
        # Flush remaining batch before exit
        try:
            db.executemany(
                "INSERT INTO bluesky_events (rev, commit_time, repo, record) VALUES (?, ?, ?, ?)",
                batch
            )
            consumer.commit(asynchronous=False)
            print(f"Inserted final batch of {len(batch)} messages.")
        except Exception as e:
            print(f"Error inserting final batch: {e}")
    consumer.close()
    db.close()
    print("Consumer closed & DB connection closed.")
