# 🌀 API Data Ingestion with Kafka & Spark Streaming

This project demonstrates a real-time data ingestion pipeline using **Apache Kafka**, **Apache Spark Streaming**, and **Apache Airflow**. The data is extracted from a public **API**, sent to Kafka, then processed by Spark Streaming, and finally stored in **Hadoop HDFS**.

## 🧱 Architecture

![Architecture]([https://github.com/hungfnguyen/lakehouse-stream-batch/issues/1#issue-3017540825](https://private-user-images.githubusercontent.com/162036529/437076112-df2175c0-cdd8-4c3a-a908-bfe62e79c007.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDU1MDc2NzYsIm5iZiI6MTc0NTUwNzM3NiwicGF0aCI6Ii8xNjIwMzY1MjkvNDM3MDc2MTEyLWRmMjE3NWMwLWNkZDgtNGMzYS1hOTA4LWJmZTYyZTc5YzAwNy5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwNDI0JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDQyNFQxNTA5MzZaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT05M2YxZjNmYWQ4ZDFmOGM1YTcyNTkyN2Q4NGY5OTI4NTZmZTQyNzQ0YmVlNDRmMTZhZWE2MzllZDM4NjZlN2Q5JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.IzQ_CjnHUMeA8YJd64sZZLiL4uGk8ar3DRLCJ75d-hQ))

### 🔁 Flow Description:

1. **API → Airflow**
   - Apache Airflow schedules and triggers the task to extract data from an external API.

2. **Airflow → Kafka (Producer)**
   - Data is published to a Kafka topic using a Python Kafka producer script.

3. **Kafka Broker + Zookeeper**
   - Kafka acts as a message broker. Zookeeper manages Kafka metadata and broker coordination.

4. **Kafka → Spark (Streaming Consumer)**
   - Apache Spark reads the data stream in real time from Kafka topics using Spark Structured Streaming.

5. **Spark → HDFS**
   - Spark processes and writes the ingested data into HDFS for further processing or analysis.

## 🛠 Tech Stack

| Component | Technology |
|----------|-------------|
| Workflow Orchestration | Apache Airflow |
| Messaging | Apache Kafka |
| Streaming Consumer | Apache Spark |
| Coordination | Apache Zookeeper |
| Storage | Hadoop HDFS |
| Language | Python (Producer) |

---

## 📁 Directory Structure

```bash
api-ingestion/
├── airflow_dags/
│   └── api_to_kafka_dag.py
├── kafka_producer/
│   └── producer.py
├── spark_streaming/
│   └── spark_kafka_consumer.py
├── docker/
│   ├── docker-compose.yml
│   └── kafka, zookeeper, spark, airflow services
├── data/
│   └── output/ (saved HDFS results)
└── README.md
```

## 📌 Authors

- **Hung Nguyen** – [@hungfnguyen](https://github.com/hungfnguyen)
