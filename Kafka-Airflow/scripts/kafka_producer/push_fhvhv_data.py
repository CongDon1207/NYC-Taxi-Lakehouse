# # import pyarrow.parquet as pq
# # import json
# # import time
# # import random
# # from kafka import KafkaProducer

# # def stream_parquet_to_kafka(parquet_file_path, kafka_bootstrap_servers, kafka_topic, sleep_time=1):
# #     print(f"Đọc dữ liệu từ: {parquet_file_path}")
# #     parquet_file = pq.ParquetFile(parquet_file_path)

# #     producer = KafkaProducer(
# #         bootstrap_servers=kafka_bootstrap_servers,
# #         value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
# #     )

# #     total_rows = parquet_file.metadata.num_rows
# #     print(f"Kết nối Kafka tại: {kafka_bootstrap_servers}")
# #     print(f"Bắt đầu gửi tới topic: {kafka_topic} ({total_rows} dòng)...")

# #     row_count = 0
# #     # Đọc toàn bộ dữ liệu 1 lần để tiện xử lý batch random (nếu file không quá lớn)
# #     table = parquet_file.read()
# #     df = table.to_pandas()
# #     total_rows = len(df)

# #     while row_count < total_rows:
# #         batch_size = random.randint(300, 1000)
# #         batch_end = min(row_count + batch_size, total_rows)
# #         batch_df = df.iloc[row_count:batch_end]

# #         for _, row in batch_df.iterrows():
# #             producer.send(kafka_topic, value=row.to_dict())
# #         producer.flush()

# #         row_count = batch_end
# #         print(f"→ Đã gửi {row_count}/{total_rows} dòng (batch size={batch_size})")
# #         time.sleep(sleep_time)

# #     print("Gửi xong toàn bộ dữ liệu.")
# import pyarrow.dataset as ds
# import json
# import time
# from kafka import KafkaProducer

# def stream_parquet_to_kafka(parquet_file_path, kafka_bootstrap_servers, kafka_topic, sleep_time=1, batch_min=300, batch_max=1000):
#     print(f"Đọc dữ liệu từ: {parquet_file_path}")
    
#     dataset = ds.dataset(parquet_file_path, format="parquet")
#     producer = KafkaProducer(
#         bootstrap_servers=kafka_bootstrap_servers,
#         value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
#     )

#     print(f"Kết nối Kafka tại: {kafka_bootstrap_servers}")
# # <<<<<<< dev/MinhNhat
# #     print(f"Bắt đầu gửi tới topic: {kafka_topic}...")

# #     total_rows = 0
# #     for batch in dataset.to_batches(batch_size=batch_max):
# #         df = batch.to_pandas()

# #         # Random kích thước batch thực tế mỗi vòng
# #         actual_batch_size = random.randint(batch_min, batch_max)
# #         for i in range(0, len(df), actual_batch_size):
# #             batch_df = df.iloc[i:i+actual_batch_size]
# #             for _, row in batch_df.iterrows():
# #                 producer.send(kafka_topic, value=row.to_dict())
# #             producer.flush()

# #             total_rows += len(batch_df)
# #             print(f"→ Đã gửi {total_rows} dòng (batch size={len(batch_df)})")
# #             time.sleep(sleep_time)

# #     print(f"✅ Gửi xong toàn bộ dữ liệu: {total_rows} dòng.")
# #     producer.close()
# # =======
#     print(f"Bắt đầu gửi tới topic: {kafka_topic} ({total_rows} dòng)...")

#     row_count = 0
#     for batch in parquet_file.iter_batches(batch_size=1000):
#         records = batch.to_pydict()
#         for i in range(len(records["hvfhs_license_num"])):
#             row = {col: records[col][i] for col in records}
#             producer.send(kafka_topic, value=row)
#             row_count += 1

#         producer.flush()  # flush sau mỗi batch
#         print(f"→ Đã gửi {row_count}/{total_rows} dòng")
#         time.sleep(sleep_time)  # sleep sau mỗi batch

#     print("Gửi xong toàn bộ dữ liệu.")
# # >>>>>>> develop





import pyarrow.dataset as ds
import json
import time
import random
from kafka import KafkaProducer

def stream_parquet_to_kafka(parquet_file_path, kafka_bootstrap_servers, kafka_topic, sleep_time=1, batch_min=300, batch_max=1000):
    print(f"Đọc dữ liệu từ: {parquet_file_path}")
    dataset = ds.dataset(parquet_file_path, format="parquet")

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

    print(f"Kết nối Kafka tại: {kafka_bootstrap_servers}")
    print(f"Bắt đầu gửi tới topic: {kafka_topic}...")

    total_sent = 0

    for batch in dataset.to_batches(batch_size=batch_max):
        df = batch.to_pandas()

        # Random kích thước batch thực tế mỗi vòng gửi
        actual_batch_size = random.randint(batch_min, batch_max)

        for i in range(0, len(df), actual_batch_size):
            batch_df = df.iloc[i:i+actual_batch_size]

            for _, row in batch_df.iterrows():
                producer.send(kafka_topic, value=row.to_dict())

            producer.flush()
            total_sent += len(batch_df)
            print(f"→ Đã gửi {total_sent} dòng (batch size = {len(batch_df)})")
            time.sleep(sleep_time)

    print(f"Gửi xong toàn bộ dữ liệu: {total_sent} dòng.")
    producer.close()

