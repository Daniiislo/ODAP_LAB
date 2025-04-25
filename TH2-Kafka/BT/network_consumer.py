from kafka import KafkaConsumer
from datetime import datetime
import json

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'network_status'

# Dictionary lưu trạng thái hiện tại của tất cả thiết bị
device_statuses = {}

def create_kafka_consumer():
    """Tạo và trả về một Kafka consumer."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset = 'latest',
        group_id = 'network_monitoring_group'
    )

def format_status_message(message):
    """Format thông điệp trạng thái mạng để hiển thị."""
    timestamp = datetime.fromisoformat(message["timestamp"])
    formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    status_text = f"[{formatted_time}] {message['device_id']} ({message['device_type']}) at {message['location']} - Status: {message['status']}"

    # Màu khác nhau cho các trạng thái khác nhau
    if message['status'] == "Online":
        status_text = f"\033[92m{status_text}\033[0m"  # Màu xanh lá cho Online
    else:
        status_text = f"\033[91m{status_text}\033[0m"  # Màu đỏ cho Offline

    # Thêm thông tin bổ sung nếu có
    additional_info = []
    for key, value in message.items():
        if key not in ["device_id", "device_type", "location", "status", "timestamp"]:
            additional_info.append(f"{key}: {value}")

        if additional_info:
            status_text += f"\n    {' | '.join(additional_info)}"
        
    return status_text
    
def process_network_status(message):
    """Xử lý thông điệp trạng thái mạng và cập nhật device_statuses."""
    device_id = message["device_id"]
    current_status = message["status"]

    # Kiểm tra xem trạng thái có thay đổi không
    previous_status = device_statuses.get(device_id, {}).get("status")
    status_changed = previous_status is not None and previous_status != current_status

    # Cập nhật trạng thái thiết bị
    device_statuses[device_id] = message

    # Hiển thị thông tin trạng thái
    print(format_status_message(message))

    if status_changed:
        print(f"\033[93mStatus changed for {device_id}: {previous_status} -> {current_status}\033[0m")  # Màu vàng cho trạng thái thay đổi

def main():
    consumer = create_kafka_consumer()

    try:
        print("Starting network status consumer...")
        for message in consumer:
            process_network_status(message.value)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()