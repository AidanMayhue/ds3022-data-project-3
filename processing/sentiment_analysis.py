import duckdb
import json
import logging
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DB_PATH = "events.duckdb"
TABLE = "events"

def ensure_compound_column(conn):
    # Query table schema to check if sentiment_compound exists
    result = conn.execute(f"PRAGMA table_info('{TABLE}');").fetchall()
    col_names = [row[1] for row in result]  # row[1] is column name
    if "sentiment_compound" not in col_names:
        logging.info("Adding sentiment_compound column to table.")
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN sentiment_compound DOUBLE;")
    else:
        logging.info("Column sentiment_compound already exists — skipping ALTER TABLE.")

def main():
    conn = duckdb.connect(DB_PATH)

    # Ensure sentiment_label column exists
    conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS sentiment_label VARCHAR;")
    # Ensure sentiment_compound exists
    ensure_compound_column(conn)

    nltk.download('vader_lexicon', quiet=True)
    sia = SentimentIntensityAnalyzer()

    # Process each record in the events table
    rows = conn.execute("SELECT kafka_key, event_json FROM events").fetchall()
    count = 0
    for kafka_key, raw in rows:
        try:
            obj = raw if isinstance(raw, dict) else json.loads(raw)
            commit = obj.get("commit")
            if not commit:
                continue
            record = commit.get("record")
            if not (record and isinstance(record, dict)):
                continue
            text = record.get("text", "")
            if not text:
                continue

            scores = sia.polarity_scores(text)
            compound = scores['compound']
            if compound >= 0.05:
                label = "positive"
            elif compound <= -0.05:
                label = "negative"
            else:
                label = "neutral"

            conn.execute(
                f"UPDATE {TABLE} SET sentiment_label = ?, sentiment_compound = ? WHERE kafka_key = ?",
                (label, float(compound), kafka_key)
            )
            count += 1

        except Exception as e:
            logging.warning(f"Failed to process record key={kafka_key}: {e}")

    logging.info(f"Processed sentiment for {count} events")


if __name__ == "__main__":
    main()
