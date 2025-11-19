import os
import duckdb
from quixstreams import Application
import json
import duckdb

def main():
    # Connect to DuckDB (creates file if it doesn't exist)
    conn = duckdb.connect("events.duckdb")

    # Create table if it doesn't already exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            kafka_key TEXT,
            kafka_offset BIGINT,
            event_json JSON
        )
    """)

app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="bluesky-duck-consumer",
    consumer_extra_config={
        "broker.address.family": "v4",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False   # manually commit after DB write
    }
)

topic = app.topic(
    name="bluesky-events",
    value_deserializer="json"
)

consumer = app.get_consumer()
consumer.subscribe([topic.name])

# ----------------------------
# DuckDB Setup
# ----------------------------
db = duckdb.connect("bluesky.db")

# Create table if not exists
db.execute("""
CREATE TABLE IF NOT EXISTS bluesky_events (
    rev TEXT,
    commit_time TIMESTAMP,
    repo TEXT,
    record JSON -- or VARCHAR if you prefer
)
""")

print("Consumer started. Writing messages into DuckDB...\n")

# ----------------------------
# Main Loop
# ----------------------------
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        
        # Deserialize message
        key = topic.deserialize_key(msg.key())
        value = topic.deserialize_value(msg.value())

        # Extract fields of interest
        rev = key
        commit_time = value["commit"]["time"]
        repo = value["commit"]["repo"]
        record_json = value  # full JSON object

        # Insert into DuckDB
        db.execute(
            """
            INSERT INTO bluesky_events (rev, commit_time, repo, record)
            VALUES (?, ?, ?, ?)
            """,
            [rev, commit_time, repo, record_json]
        )

        # Important: commit Kafka offset only after a successful insert
        consumer.commit(asynchronous=False)

        print(f"Inserted rev={rev} into DuckDB.")

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    db.close()
    print("Consumer closed & DB connection closed.")
