#!/bin/bash
# Script để tiêu thụ dữ liệu từ Kafka topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic nyc_taxi_stream --from-beginning
