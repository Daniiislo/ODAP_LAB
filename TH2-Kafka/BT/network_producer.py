from kafka import KafkaProducer
from datetime import datetime
import json
import random
import time

#Cấu hình kafka
KAFKA_BOOSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'network_status'

# Danh sách một vài thiết bị mẫu
DEVICES = [
    {"id" : "server-001", "type": "server", "location": "Room A"},
    {"id" : "router-001", "type": "router", "location": "Floor 1"},
    {"id" : "switch-001", "type": "switch", "location": "Network Room"},
    {"id" : "server-002", "type": "server", "location": "Room B"},
    {"id" : "router-002", "type": "router", "location": "Floor 2"}
]

def create_kafka_producer():
    """Tạo và trả về một Kafka producer."""
    return KafkaProducer(
        bootstrap_servers = KAFKA_BOOSTRAP_SERVERS,
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

def generate_network_status():
    """Tạo thông tin trạng thái mạng giả lập cho một thiết bị ngẫu nhiên."""

    device = random.choice(DEVICES)
    status = random.choice(["Online", "Offline"])

    # Tùy thuộc vào từng loại thiết bị, generate thêm thông tin bổ sung cho thiết bị đó
    additional_info = {}
    if device["type"] == "server":
        additional_info["cpu_usage"] = f"{random.randint(0, 100)}%"
        additional_info["memory_usage"] = f"{random.randint(0, 100)}%"
    elif device["type"] == "router":
        additional_info["bandwidth"] = f"{random.randint(10, 1000)} Mbps"
    elif device["type"] == "switch":
        additional_info["active_ports"] = f"{random.randint(1, 24)}/24"


    # Tạo thông điệp
    message = {
        "device_id": device["id"],
        "device_type": device["type"],
        "location": device["location"],
        "status": status,
        "timestamp": datetime.now().isoformat(),
        **additional_info
    }

    return message

def main():
    # Tạo Kafka producer
    producer = create_kafka_producer()

    try:
        print("Starting network status producer...")
        while True:
            # Tạo thông điệp trạng thái mạng
            message = generate_network_status()

            # Gửi thông điệp đến Kafka
            producer.send(KAFKA_TOPIC, value=message)

            # In thông điệp đã gửi
            print(f"Sent: {message['device_id']} ({message['device_type']}) - Status: {message['status']}")

            # Đợi một khoảng thời gian trước khi gửi thông điệp tiếp theo
            time.sleep(random.randint(1, 5))

    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        # Đảm bảo tất cả thông điệp được gửi
        producer.flush()
        # Đóng producer
        producer.close()

if __name__ == "__main__":
    main()  
