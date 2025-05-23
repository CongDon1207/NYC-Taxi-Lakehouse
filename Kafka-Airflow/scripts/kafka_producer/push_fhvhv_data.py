import pyarrow.parquet as pq
import json
import time
import random
from kafka import KafkaProducer

def stream_parquet_to_kafka(parquet_file_path, kafka_bootstrap_servers, kafka_topic, sleep_time=1):
    print(f"Đọc dữ liệu từ: {parquet_file_path}")
    parquet_file = pq.ParquetFile(parquet_file_path)

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

    total_rows = parquet_file.metadata.num_rows
    print(f"Kết nối Kafka tại: {kafka_bootstrap_servers}")
    print(f"Bắt đầu gửi tới topic: {kafka_topic} ({total_rows} dòng)...")

    row_count = 0
    # Đọc toàn bộ dữ liệu 1 lần để tiện xử lý batch random (nếu file không quá lớn)
    table = parquet_file.read()
    df = table.to_pandas()
    total_rows = len(df)

    while row_count < total_rows:
        batch_size = random.randint(300, 1000)
        batch_end = min(row_count + batch_size, total_rows)
        batch_df = df.iloc[row_count:batch_end]

        for _, row in batch_df.iterrows():
            producer.send(kafka_topic, value=row.to_dict())
        producer.flush()

        row_count = batch_end
        print(f"→ Đã gửi {row_count}/{total_rows} dòng (batch size={batch_size})")
        time.sleep(sleep_time)

    print("Gửi xong toàn bộ dữ liệu.")
