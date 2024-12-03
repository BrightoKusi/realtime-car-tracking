# realtime-car-tracking

## Overview
This project simulates a real-time vehicle tracking system and implements a streaming pipeline using Apache Kafka, Apache Spark Structured Streaming, and AWS S3. The pipeline processes and stores vehicle-related data from Accra to Kumasi in Ghana. The system handles five types of data streams: vehicle information, GPS information, camera information, weather information, and emergency alerts.

## Architecture
1. Data Simulation:
Python scripts generate real-time data for vehicles traveling between Accra and Kumasi.
Data is pushed to Kafka topics for each stream.

2. Streaming Platform:
Apache Kafka manages the real-time data streams.
Kafka topics include:
vehicle_info
gps_info
camera_info
weather_info
emergency_info

3. Stream Processing:
Spark Structured Streaming processes the data in real-time.
Data schemas ensure structured and consistent ingestion.

4. Data Storage:
Processed data is written to AWS S3 in Parquet format for efficient querying and long-term storage.


## Setup Instructions
1. Prerequisites
Docker & Docker Compose
Python 3.11+
AWS account with access to S3
Kafka and Spark knowledge

2. Clone Repository
\`\`\`bash
git clone <repository_url>
cd <repository_name>
\`\`\`

3. Configure AWS Credentials
Create a file jobs/config.py containing your AWS credentials:
\`\`\`python
aws_credentials = {
    "AWS_ACCESS_KEY": "<your-access-key>",
    "AWS_SECRET_KEY": "<your-secret-key>"
} 
\`\`\`

4. Start Kafka and Spark Cluster
Navigate to the docker-compose.yml file and launch the containers:
\`\`\`bash
docker-compose up -d
\`\`\`bash

5. Run the Data Simulation
Execute the Python script to start simulating data:
\`\`\`python
python jobs/spark-stream.py
\`\`\`

6. Start Spark Streaming Job
Submit the Spark job to process data from Kafka and write to S3:
\`\`\`python
spark-write.py
\`\`\`

