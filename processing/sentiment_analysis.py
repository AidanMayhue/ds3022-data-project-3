import duckdb
import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    nltk.download('vader_lexicon')
except Exception as e:
    logging.warning(f"Failed to download NLTK data: {e}")

try:
    conn = duckdb.connect("events.duckdb")
except Exception as e:
    logging.error(f"Could not connect to DuckDB: {e}")
    raise

try:
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS sentiment_label TEXT")
except Exception as e:
    logging.error(f"Error adding sentiment_label column: {e}")

sia = SentimentIntensityAnalyzer()

try:
    events = conn.execute("SELECT rowid, event_json FROM events").fetchall()
except Exception as e:
    logging.error(f"Failed to fetch events: {e}")
    events = []

for rowid, event_json in events:
    try:
        text = event_json.get("text", "") if isinstance(event_json, dict) else ""
        score = sia.polarity_scores(text)
        if score['compound'] >= 0.05:
            sentiment = "positive"
        elif score['compound'] <= -0.05:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        conn.execute("UPDATE events SET sentiment_label=? WHERE rowid=?", (sentiment, rowid))
    except Exception as e:
        logging.warning(f"Failed to process row {rowid}: {e}")

logging.info("Sentiment analysis completed!")
