import pyarrow.parquet as pq
import json
import time
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
    for batch in parquet_file.iter_batches(batch_size=1000):
        records = batch.to_pydict()
        for i in range(len(records["hvfhs_license_num"])):
            row = {col: records[col][i] for col in records}
            producer.send(kafka_topic, value=row)
            row_count += 1

        producer.flush()  # flush sau mỗi batch
        print(f"→ Đã gửi {row_count}/{total_rows} dòng")
        time.sleep(sleep_time)  # sleep sau mỗi batch

    print("Gửi xong toàn bộ dữ liệu.")