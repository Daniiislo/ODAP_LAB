from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'user-events'

# Định nghĩa các loại sự kiện
EVENT_TYPES = ["click", "view", "purchase"]
EVENT_WEIGHTS = [0.6, 0.3, 0.1]  # click: 60%, view: 30%, purchase: 10%


USERS = [f"user_{i}" for i in range(1, 10)]  # Đúng: một list các user IDs
ITEMS = [f"item_{i}" for i in range (1, 20)] # Danh sách các mặt hàng mẫu

def event_partitioner(key_bytes, all_partitions, available_partitions):
    """
    Hàm phân chia sự kiện vào các partition dựa trên loại sự kiện:
    - click vào partition 0
    - view vào partition 1
    - purchase vào partition 2
    """
    event_type = key_bytes.decode('utf-8')
    if event_type == 'click':
        return 0
    elif event_type == 'view':
        return 1
    elif event_type == 'purchase':
        return 2
    else:
        # Mặc định cho các loại sự kiện khác
        return 0

def create_kafka_producer():
    """Tạo và trả về Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        partitioner=event_partitioner
    )

def generate_user_event():
    """Tạo một sự kiện người dùng giả lập."""
    user_id = random.choice(USERS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    item_id = random.choice(ITEMS)
    
    event = {
        "user_id": user_id,
        "event_type": event_type,
        "item_id": item_id,
        "timestamp": datetime.now().isoformat()
    }
    
    return event, event_type

def main():
    producer = create_kafka_producer()
    
    try:
        print("Starting producing events...")
        
        while True:
            # Tạo sự kiện người dùng
            event, event_type = generate_user_event()
            
            # Gửi sự kiện đến Kafka topic với key chứa event_type
            future = producer.send(KAFKA_TOPIC, key=event_type, value=event)
            
            # Lấy kết quả để kiểm tra partition
            metadata = future.get(timeout=10)
            partition = metadata.partition
            
            # In thông tin sự kiện đã gửi
            print(f"[{event['timestamp']}] Sent: {event['user_id']} - {event['event_type']} - {event['item_id']} - (Partition: {partition})")
                
            # Ngủ một khoảng thời gian ngẫu nhiên trước khi gửi sự kiện tiếp theo
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\nProducer stopped by user")
    finally:
        producer.flush()  # Đảm bảo tất cả messages được gửi
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    main()
