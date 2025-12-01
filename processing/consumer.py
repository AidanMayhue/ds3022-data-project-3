# Import Quix Streams, DuckDB, OS utilities, and JSON parsing
from quixstreams import Application
import duckdb
import os
import json

# Resolve path to DuckDB file and print for debugging
DB_PATH = os.path.abspath("events.duckdb")
print(f"Using DuckDB file: {DB_PATH}")

# Connect to DuckDB database file (creates file if missing)
conn = duckdb.connect(DB_PATH)

conn.execute("DROP TABLE IF EXISTS events;")  # remove previous table so reruns are clean


# Create the events table (if it doesn't exist after DROP)
conn.execute("""
    CREATE TABLE IF NOT EXISTS events (
        kafka_key TEXT,
        kafka_offset BIGINT,
        event_json JSON
    )
""")
print("Verified table 'events' exists.")

# Initialize Quix Streams application as a Kafka consumer
app = Application(
    broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
    loglevel="DEBUG",              # show detailed logs
    consumer_group="bluesky-consumer",  # Kafka consumer group ID
    auto_offset_reset="earliest",  # start from earliest if no committed offset
    producer_extra_config={"broker.address.family": "v4"}  # ensure IPv4
)

# Create a consumer and subscribe to the topic
with app.get_consumer() as consumer:
    consumer.subscribe(["bluesky-events"])
    print("Subscribed to topic 'bluesky-events'...")

    try:
        while True:
            # Poll Kafka for one message
            msg = consumer.poll(1)
            if msg is None:
                continue  # nothing available yet

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue  # skip malformed or empty messages

            try:
                # Decode Kafka message key/value
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8")  # raw JSON string

                # Insert the record into DuckDB table
                conn.execute(
                    "INSERT INTO events VALUES (?, ?, ?)",
                    (key, msg.offset(), value)
                )

                # Commit the offset to Kafka
                consumer.store_offsets(msg)
                print(f"Inserted offset={msg.offset()}, key={key}")
            except Exception as e:
                print(f"Error inserting message: {e}")

    except KeyboardInterrupt:
        print("\nStopped by user.")
