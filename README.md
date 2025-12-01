## Bluesky Data Pipeline

This project is an end-to-end pipeline for collecting, storing, analyzing, and visualizing posts from the Bluesky social network. It shows how live social media data can flow from a WebSocket feed into a database, be analyzed, and finally explored in an interactive dashboard.

## How It Works

Producer:
Connects to Bluesky’s feed and streams new posts in real time, sending them to a Kafka topic. This is the first step, because without it no data flows through the pipeline.

Consumer:
Reads messages from Kafka and stores them in a DuckDB database. Each post is saved with metadata, creating a reliable history of events.

Embeddings:
Extracts the text from posts and computes semantic embeddings with a pre-trained model. These vectors make it easy to compare, search, or analyze posts based on meaning.

Sentiment Analysis:
Scores each post for sentiment (positive, negative, neutral) and stores both labels and numeric scores in the database. This adds an emotional dimension to the data.

Dashboard:
Provides an interactive interface to explore the data: see sentiment distributions, top positive/negative posts, and a word cloud of common words.

## Running the Pipeline

Start the producer to stream data, then run the consumer to store it. After you have some posts collected, run embeddings and sentiment analysis to process the content. Finally, open the dashboard to explore the results visually.

This pipeline demonstrates a full real-time data workflow, from ingesting live posts to analyzing content and visualizing insights.

## Project Overview
See the project overview folder for a more detailed summary of our work :)
