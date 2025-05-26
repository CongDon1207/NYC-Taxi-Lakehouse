import polars as pl
import json
import time
import random
from kafka import KafkaProducer

def stream_parquet_to_kafka_with_polars(parquet_file_path, kafka_bootstrap_servers, kafka_topic, sleep_time=1, batch_min=300, batch_max=1000):
    print(f"Đọc dữ liệu từ: {parquet_file_path}")

    # Đọc file parquet theo streaming
    df = pl.read_parquet(parquet_file_path)

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

    print(f"Kết nối Kafka tại: {kafka_bootstrap_servers}")
    print(f"Bắt đầu gửi tới topic: {kafka_topic}...")

    total_sent = 0

    df = df.sample(fraction=1.0) 

    num_rows = df.shape[0]
    idx = 0

    while idx < num_rows:
        actual_batch_size = random.randint(batch_min, batch_max)
        end_idx = min(idx + actual_batch_size, num_rows)

        batch_df = df.slice(idx, end_idx - idx).to_dicts()

        for row in batch_df:
            producer.send(kafka_topic, value=row)

        producer.flush()
        sent_count = len(batch_df)
        total_sent += sent_count
        print(f"→ Đã gửi {total_sent} dòng (batch size = {sent_count})")
        time.sleep(sleep_time)

        idx = end_idx

    print(f"Gửi xong toàn bộ dữ liệu: {total_sent} dòng.")
    producer.close()
