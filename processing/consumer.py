from quixstreams import Application
import json
import duckdb
import os

def main():

    # ALWAYS USE AN ABSOLUTE PATH
    db_path = os.path.abspath("events.duckdb")
    print(f"Using DuckDB file: {db_path}")

    conn = duckdb.connect(db_path)

    # Force-create the table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            kafka_key TEXT,
            kafka_offset BIGINT,
            event_json JSON
        )
    """)
    print("Verified: table 'events' exists.")

    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="bluesky-consumer",
        auto_offset_reset="earliest",
        producer_extra_config={"broker.address.family": "v4"}
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["bluesky-events"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
                continue

            if msg.error():
                raise Exception(msg.error())

            key = msg.key().decode("utf8") if msg.key() else None
            value = json.loads(msg.value())
            offset = msg.offset()

            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?)",
                (key, offset, json.dumps(value))
            )

            print(f"Inserted offset={offset}, key={key}")

            consumer.store_offsets(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")

