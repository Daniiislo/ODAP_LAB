from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'app-logs'

# Danh sách các dịch vụ mẫu
SERVICES = [
    "user-service", 
    "payment-service", 
    "order-service", 
    "inventory-service", 
    "notification-service"
]

# Các mức độ log
LOG_LEVELS = ["INFO", "ERROR", "DEBUG"]

# Xác suất xuất hiện của các loại log (ERROR ít hơn các loại khác)
LOG_LEVEL_WEIGHTS = [0.6, 0.1, 0.3]  # INFO: 60%, ERROR: 10%, DEBUG: 30%

LOG_MESSAGES = {
    "INFO": [
        "User {} logged in successfully",
        "Transaction {} completed",
        "New order {} created",
        "Item {} added to inventory",
        "Email notification sent to {}"
    ],
    "ERROR": [
        "Failed to authenticate user {}",
        "Payment processing error for transaction {}",
        "Order {} could not be fulfilled",
        "Database connection failed for {}",
        "Service {} unavailable"
    ],
    "DEBUG": [
        "Processing request for user {}",
        "Validating payment details for {}",
        "Checking inventory for item {}",
        "Query execution time: {} ms",
        "Cache hit ratio: {}%"
    ]
}

def create_kafka_producer():
    """Tạo và trả về Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_log_entry():
    """Tạo một bản ghi log giả lập."""
    service = random.choice(SERVICES)
    log_level = random.choices(LOG_LEVELS, weights=LOG_LEVEL_WEIGHTS, k=1)[0]

    message_template = random.choice(LOG_MESSAGES[log_level])
    message_content = message_template.format(
        random.randint(1000, 9999),  # ID người dùng, giao dịch, đơn hàng, v.v ngẫu nhiên.
    )

    # Tạo thông điệp log
    log_entry = {
        "log_level": log_level,
        "service_name": service,
        "message": message_content,
        "timestamp": datetime.now().isoformat()
    }

    return log_entry, log_level

def main():
    producer = create_kafka_producer()

    try:
        print("Starting to produce logs...")
        while True:
            # Tạo bản ghi log
            log_entry, log_level = generate_log_entry()

            # Tạo headers với metadata (mức độ log)
            headers = [
                ("log_level", log_level.encode('utf-8')),
            ]

            # Gửi bản ghi log đến Kafka
            producer.send(
                KAFKA_TOPIC,
                value = log_entry,
                headers = headers
            )

            # In thông điệp tin log đã gửi
            print(f"[{log_entry['timestamp']}] Sent: {log_entry['service_name']} - {log_entry['log_level']} - {log_entry['message']}")

            # Đợi một khoảng thời gian ngẫu nhiên trước khi gửi bản ghi tiếp theo
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("Log production stopped.")
    finally:
        producer.flush()
        # Đóng producer
        producer.close()
        print("Kafka producer closed.")

if __name__ == "__main__":
    main()