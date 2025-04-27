from kafka import KafkaProducer
import json

class KafkaMessageProducer:
    def __init__(self, kafka_bootstrap_servers: str, kafka_topic: str):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize message thành JSON
        )
        self.topic = kafka_topic

    def send_message(self, message: dict):
        """Gửi 1 message (dict) vào Kafka."""
        self.producer.send(self.topic, value=message)
        self.producer.flush()  # Đẩy message ra ngay (không buffer lâu)
        print(f"[KafkaProducer] Sent message to topic `{self.topic}`: {message}")

    def close(self):
        """Đóng kết nối Kafka."""
        self.producer.close()
        print("[KafkaProducer] Connection closed.")