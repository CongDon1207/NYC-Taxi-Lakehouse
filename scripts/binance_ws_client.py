import websocket
import json
import threading
import time
from kafka_producer import KafkaMessageProducer

class BinanceWebSocketClient:
    def __init__(self, stream_url, kafka_bootstrap_servers, kafka_topic):
        self.stream_url = stream_url
        self.kafka_producer = KafkaMessageProducer(kafka_bootstrap_servers, kafka_topic)
        self.ws = None

    def on_open(self, ws):
        print("[WebSocket] Connection opened.")

    def on_message(self, ws, message):
        print("[WebSocket] Received Message:")
        print(message)

        try:
            parsed_message = json.loads(message)  # Parse JSON string thành dict
            self.kafka_producer.send_message(parsed_message)
        except Exception as e:
            print(f"[WebSocket] Error while processing message: {e}")

    def on_error(self, ws, error):
        print("[WebSocket] Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("[WebSocket] Connection closed:", close_status_code, close_msg)
        self.kafka_producer.close()

    def start(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            self.stream_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        # Run websocket forever in a separate thread
        wst = threading.Thread(target=self.ws.run_forever)
        wst.daemon = True
        wst.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Interrupted. Closing WebSocket...")
            self.ws.close()
            self.kafka_producer.close()

if __name__ == "__main__":
    # WebSocket stream URL
    stream_name = "btcusdt@trade"  # Cặp giao dịch BTC/USDT real-time
    ws_url = f"wss://stream.binance.com:9443/ws/{stream_name}"

    # Kafka configuration
    kafka_bootstrap_servers = "broker:29092"
    kafka_topic = "binance_trades"

    client = BinanceWebSocketClient(
        stream_url=ws_url,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic
    )
    client.start()
