# Hệ thống Theo dõi Mạng Thời gian thực sử dụng Kafka

Đây là ứng dụng theo dõi trạng thái mạng thời gian thực sử dụng Apache Kafka. Hệ thống gồm một producer gửi thông tin trạng thái thiết bị mạng và một consumer nhận và xử lý thông tin này.

## Yêu cầu

- Python 3.6+
- Apache Kafka

## Cài đặt

1. Cài đặt thư viện cần thiết:

```
pip install kafka-python
```

2. Đảm bảo Kafka đang chạy trên localhost:9092

3. Tạo Kafka topic (nếu chưa có):

```
kafka-topics.sh --create --topic network_status --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Cách sử dụng

1. Chạy producer để giả lập việc gửi thông tin trạng thái mạng:

```
python network_producer.py
```

2. Trong một terminal khác, chạy consumer để nhận và xử lý thông tin:

```
python network_consumer.py
```

## Tính năng

- Producer giả lập việc gửi thông tin trạng thái từ nhiều thiết bị mạng
- Consumer hiển thị trạng thái "Online" hoặc "Offline" với màu sắc khác nhau
- Theo dõi thay đổi trạng thái và cảnh báo khi thiết bị chuyển sang offline
- Lưu trữ trạng thái hiện tại của tất cả thiết bị
