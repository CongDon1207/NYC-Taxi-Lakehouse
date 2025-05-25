from kafka import KafkaProducer
import json
import time

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Nhập dữ liệu gửi lên Kafka")

try:
    while True:
        id_input = input("Nhập id: ")
        message_input = input("Nhập message: ")

        data = {
            "id": id_input,
            "message": message_input
        }
        producer.send('miniotopic', value=data)
        print(f"Sent: {data}")

except KeyboardInterrupt:
    print("\nThoát")

finally:
    producer.flush()
    producer.close()
    print("Producer đã được đóng.")
