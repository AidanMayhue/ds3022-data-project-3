import os
import json
import duckdb
from quixstreams import Application

KAFKA_BROKER = "localhost:9092"

# ----------------------------
# Kafka setup
# ----------------------------
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="bluesky-duck-consumer",
    consumer_extra_config={
        "broker.address.family": "v4",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
)

topic = app.topic(
    name="bluesky-events",
    value_deserializer="json"
)

consumer = app.get_consumer()
consumer.subscribe([topic.name])

# ----------------------------
# DuckDB setup
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

print("Consumer started. Writing messages into DuckDB...\n")

# ----------------------------
# Main Loop
# ----------------------------
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        
        key = topic.deserialize_key(msg.key())
        value = topic.deserialize_value(msg.value())

        rev = key
        commit_time = value["commit"]["time"]
        repo = value["commit"]["repo"]
        record_json = json.dumps(value)

        db.execute(
            """
            INSERT INTO bluesky_events (rev, commit_time, repo, record)
            VALUES (?, ?, ?, ?)
            """,
            [rev, commit_time, repo, record_json]
        )

        consumer.commit(asynchronous=False)
        print(f"Inserted rev={rev} into DuckDB.")

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    db.close()
    print("Consumer closed & DB connection closed.")
