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
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="bluesky-consumer",
        auto_offset_reset="earliest",
        producer_extra_config={
            "broker.address.family": "v4"
        }
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["bluesky-events"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
                continue

            if msg.error() is not None:
                raise Exception(msg.error())

            # Parse message
            key = msg.key().decode("utf8") if msg.key() else None
            value = json.loads(msg.value())
            offset = msg.offset()

            # Insert into DuckDB
            conn.execute(
                """
                INSERT INTO events (kafka_key, kafka_offset, event_json)
                VALUES (?, ?, ?)
                """,
                (key, offset, json.dumps(value))
            )

            # Print for debugging
            print(f"Stored offset={offset}, key={key}")

            # Commit Kafka offset
            consumer.store_offsets(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")

