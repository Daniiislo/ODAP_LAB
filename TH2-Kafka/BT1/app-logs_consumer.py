from kafka import KafkaConsumer
import json
import logging
import os
import time
import threading
from datetime import datetime

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'app-logs'
CONSUMER_GROUP = 'app-logs-consumer-group'

# Cấu hình thư mục lưu trữ log
ERROR_LOG_FILE = "error_logs.txt"

logging.basicConfig(
    level = logging.ERROR,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler(ERROR_LOG_FILE)
    ]
)

logger = logging.getLogger("error_logger")

# Biến toàn cục đếm số lượng log theo từng mức độ
log_count = {
    'INFO': 0,
    'ERROR': 0,
    'DEBUG': 0,
    'TOTAL': 0
}

# Lock để đồng bộ hóa truy cập vào biến log_count
lock_count = threading.Lock()

def create_kafka_consumer():
    """Tạo và trả về Kafka consumer."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer = lambda m: json.loads(m.decode('utf-8')),
        group_id = CONSUMER_GROUP,
        auto_offset_reset = 'earliest',
        enable_auto_commit = True,
    )

def log_statistics_reporter():
    """Hàm báo cáo thống kê log sau mỗi 10 giây."""

    global log_count

    while True:
        time.sleep(10)

        with lock_count: #Khóa lại để đảm bảo an toàn khi truy cập vào biến log_count
            current_counts = log_count.copy()
            total = current_counts['TOTAL']

        if total == 0:
            continue

        # Tính toán tỷ lệ phần trăm cho từng mức độ log
        info_percentage = (current_counts['INFO'] / total) * 100
        error_percentage = (current_counts['ERROR'] / total) * 100
        debug_percentage = (current_counts['DEBUG'] / total) * 100

        # In ra thống kê
        print ("\n" + "="*50)
        print (f"LOG STATISTICS REPORT IN LAST 10 SECONDS: Total logs: {total}")
        print (f"INFO: {current_counts['INFO']} ({info_percentage:.2f}%)")
        print (f"ERROR: {current_counts['ERROR']} ({error_percentage:.2f}%)")
        print (f"DEBUG: {current_counts['DEBUG']} ({debug_percentage:.2f}%)")
        print ("="*50 + "\n")

def process_log(message):
    """Xử lý log và cập nhật thống kê."""
    global log_count

    # Giải mã thông điệp log
    log_data = message.value

    headers = dict([(k, v.decode('utf-8')) for k, v in message.headers])
    log_level = headers.get("log_level")
    service_name = log_data["service_name"]
    message_content = log_data["message"]
    timestamp = log_data["timestamp"]

    with lock_count: #Khóa lại để đảm bảo an toàn khi truy cập vào biến log_count
        log_count[log_level] += 1
        log_count['TOTAL'] += 1
    
    # XỬ lý riêng các log ERROR
    if log_level == 'ERROR':
        log_message = f"{service_name} - {message_content}"
        logger.error(log_message)

    # In thông tin log đã nhận được
    level_color = "\033[0m"  # Mặc định không có màu
    if log_level == 'INFO':
        level_color = "\033[92m"  # Màu xanh lá
    elif log_level == 'ERROR':
        level_color = "\033[91m"  # Màu đỏ
    elif log_level == 'DEBUG':
        level_color = "\033[94m"  # Màu xanh dương

    print (f"[{timestamp}] {service_name}: {level_color}{log_level}\033[0m - {message_content}")


def main():
    with open (ERROR_LOG_FILE, "a") as f:
        pass # Tạo file nếu chưa tồn tại

    # Khởi động thread báo cáo thống kê
    stats_thread = threading.Thread(target=log_statistics_reporter, daemon=True)
    stats_thread.start()

    # Tạo Kafka consumer
    consumer = create_kafka_consumer()

    try:
        print ("Starting to consume logs...")
        print ("ERROR logs will be saved to:", ERROR_LOG_FILE)
        print ("Waiting for messages...")

        for message in consumer:
            process_log(message)
    except KeyboardInterrupt:
        print ("\nStopping consumer...")
    finally:
        consumer.close()
        print ("Consumer closed.")

if __name__ == "__main__":
    main()