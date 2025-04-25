# Hệ thống Thu thập Log Tập trung sử dụng Kafka

Đây là một ứng dụng thu thập và xử lý log tập trung sử dụng Apache Kafka. Hệ thống này mô phỏng việc thu thập log từ nhiều dịch vụ khác nhau, phân tách log lỗi để xử lý ưu tiên, và thống kê số lượng log theo mức độ.

## Yêu cầu

- Python 3.6+
- Apache Kafka đang chạy
- Thư viện kafka-python (cài đặt với `pip install kafka-python`)

## Cài đặt

1. Cài đặt thư viện cần thiết:

```
pip install kafka-python
```

2. Đảm bảo Kafka đang chạy trên localhost:9092

3. Tạo Kafka topic (nếu chưa có):

```
kafka-topics.sh --create --topic app-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Cách sử dụng

1. Chạy producer để giả lập việc gửi log từ các ứng dụng:

```
python app-logs_producer.py
```

2. Trong một terminal khác, chạy consumer để xử lý log:

```
python app-logs_consumer.py
```

## Tính năng

- **Producer**: Giả lập việc tạo log từ nhiều dịch vụ khác nhau với các mức độ INFO, ERROR, DEBUG
- **Consumer**:
  - Tách riêng và lưu trữ log lỗi (ERROR) vào file error.log
  - Hiển thị log với màu sắc khác nhau dựa trên mức độ
  - Tính toán và hiển thị thống kê số lượng log theo mức độ sau mỗi 10 giây

## Ghi chú triển khai

- Sử dụng Kafka Headers để gắn metadata (mức độ log) cho mỗi thông điệp
- Có thể mở rộng bằng cách chạy nhiều consumer trong cùng consumer group để xử lý song song
- Để lưu trữ hiệu quả, nên cấu hình log compaction cho topic log
