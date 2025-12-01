# Team Double A

## Team members

Amelia Vasiliu and Aidan Mayhue

## Data Source

What data source did you work with?

BlueSky live-stream data

## Challenges / Obstacles


What challenges did this data choice present in data gathering, processing, and analysis, and how did you work through them? What methods and tools did you use to work with this data?

This project revolves around collecting data from the Bluesky firehose API for text analysis. Sentiment analysis was performed on Bluesky posts in an effort to determine the emotions associated with the post. The main challenge in gathering data for this source was successfully capturing data from the firehose without losing data. As discussed in class, we are awash in data, and effectively capturing it is the prime challenge for solutions such as this. The primary challenge in processing the data was finding an appropriate method for storage following processing. Furthermore, the large amount of data constantly flowing led to lengthy processing times. For our solution, collecting a minimum of 100,000 entries took roughly two hours, sometimes more or less, depending on Bluesky user behavior. For our final data count, we managed to collect approximately 400,000 BlueSky posts. To process and capture our data, we chose to use Kafka, utilizing both a producer and a consumer script. We opted for duckdb as our storage method following processing due to ease of use. This led to another challenge. Our project placed our posts into DuckDB in JSON format, which we found didn’t work well within our sentiment analysis script, as well as our embedding script. This was a bit of a puzzle; it wasn’t readily apparent where the issue was, so we spent a significant amount of time trying to identify the issue. Even with proper error handling, this was still a challenge because the issue was presented as an empty DuckDB table, when in reality the table was far from empty. After converting the posts from JSON to text, sentiment analysis was performed. A dashboard was finally created to visualize the data. Ensuring reproducibility was a challenge with the entire project. There is no convenient way to share a dashboard, so making sure our sequence of scripts ran appropriately posed an additional challenge. A potential future application could be using an orchestration service such as Airflow or Prefect. 

## Analysis

Offer a brief analysis of the data with your findings. Keep it to one brief, clear, and meaningful paragraph.

Plots from our dashboard showed that sentiment found in posts was predominantly neutral. The number of neutral posts is around double the number of positive posts and triple the number of negative posts. Positive and negative posts were generally scored differently, however. Positive and negative posts saw large distributions with few small variations in density. Notably, the posts that scored the highest sentiment scores on both ends of the distribution appear manic. Furthermore, the most frequent words appear to fluctuate significantly depending on when the data is collected; some are consistent, like bsky (an abbreviation for blue sky), while others, like game, appear to vary depending on the trending topic (when this data was collected, a video game was one of the trending topics).

## Plot / Visualization
<img width="1117" height="920" alt="Screenshot 2025-12-01 at 11 14 20 AM" src="https://github.com/user-attachments/assets/bb0d77f7-f3db-423e-b66f-bf560167b7df" />
<img width="1473" height="655" alt="Screenshot 2025-12-01 at 11 34 46 AM" src="https://github.com/user-attachments/assets/2d2ddc50-4877-4273-b5a6-92e9a3b3b39d" />
<img width="1231" height="511" alt="Screenshot 2025-12-01 at 11 34 55 AM" src="https://github.com/user-attachments/assets/8add63d8-6a9d-4c58-9400-cf8bcdb7e75e" />
<img width="1081" height="562" alt="Screenshot 2025-12-01 at 11 35 04 AM" src="https://github.com/user-attachments/assets/1ad60b38-e1b1-464a-9150-10a3fcb83919" />

## GitHub Repository

[https://github.com/](https://github.com/AidanMayhue/dp3-work)
