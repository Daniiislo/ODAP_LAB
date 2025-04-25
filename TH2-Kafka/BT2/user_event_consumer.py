from kafka import KafkaConsumer
import json
import time
import threading
from datetime import datetime


# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'user-events'
CONSUMER_GROUP = 'user-event-consumer'


# Biến để lưu trữ số lượng sự kiện theo loại
event_counts = {
    "click": 0,
    "view": 0,
    "purchase": 0,
    "total": 0
}

# Lock để đồng bộ hóa việc cập nhật event_counts
count_lock = threading.Lock()

def create_kafka_consumer():
    """Tạo và trả về Kafka consumer."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    return consumer

def event_statistics_reporter(interval=60):
    """Hàm báo cáo thống kê sự kiện mỗi phút."""
    global event_counts

    while True:
        time.sleep(interval)

        with count_lock:
            # Lấy giá trị hiện tại của các biến đếm
            current_counts = event_counts.copy()

            # Reset lại bộ đếm
            event_counts = {key: 0 for key in event_counts}

        if current_counts["total"] == 0:
            print("No events received in the last minute.")
            continue

        # Tính toán tỷ lệ phần trăm cho từng loại sự kiện
        click_percentage = (current_counts["click"] / current_counts["total"]) * 100
        view_percentage = (current_counts["view"] / current_counts["total"]) * 100
        purchase_percentage = (current_counts["purchase"] / current_counts["total"]) * 100

        # In ra thống kê
        print("\n" + "="*60)
        print(f"EVENT STATISTICS IN THE LAST MINUTE: Total events: {current_counts['total']}")
        print(f"CLICK: {current_counts['click']} ({click_percentage:.2f}%)")
        print(f"VIEW: {current_counts['view']} ({view_percentage:.2f}%)")
        print(f"PURCHASE: {current_counts['purchase']} ({purchase_percentage:.2f}%)")
        print("="*60 + "\n")

def process_event(message):
    """Xử lý sự kiện và cập nhật counters."""
    global event_counts

    event_data = message.value
    event_type = message.key  # Lấy event_type từ key
    partition = message.partition  # Lấy partition từ message
    
    user_id = event_data["user_id"]
    item_id = event_data["item_id"]
    timestamp = event_data["timestamp"]
    
    # Cập nhật số lượng sự kiện
    with count_lock:
        event_counts[event_type] += 1
        event_counts["total"] += 1
    
    # Xác định màu sắc để hiển thị dựa trên loại sự kiện
    event_color = "\033[0m"  # Mặc định
    if event_type == "click":
        event_color = "\033[94m"  # Xanh dương
    elif event_type == "view":
        event_color = "\033[92m"  # Xanh lá
    elif event_type == "purchase":
        event_color =  "\033[93m"  # Vàng
    
    # In thông tin sự kiện đã nhận được
    print(f"[{timestamp}] Received: {event_color}{event_type}\033[0m - User: {user_id} - Item: {item_id} (Partition: {partition})")

def main():

    # Khởi động thread báo cáo thống kê
    stats_thread = threading.Thread(target=event_statistics_reporter, args=(60,))
    stats_thread.daemon = True
    stats_thread.start()
    
    # Tạo consumer
    consumer = create_kafka_consumer()
    
    try:
        print("Starting consuming events...")
        print("Waiting for events...")
        
        # Tiêu thụ thông điệp
        for message in consumer:
            process_event(message)
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    main()
