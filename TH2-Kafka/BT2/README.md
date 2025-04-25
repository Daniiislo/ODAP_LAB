# Hệ thống Phân tích Hành vi Người dùng với Kafka

## Tổng quan

Đây là hệ thống phân tích hành vi người dùng sử dụng Apache Kafka. Hệ thống gồm một producer gửi các sự kiện người dùng với `custom partitioner` để phân loại sự kiện và một consumer để phân tích các sự kiện theo thời gian thực.

## Yêu cầu hệ thống

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
bin/kafka-topics.sh --create --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Đặc tả kĩ thuật

### 1. Producer (user_event_producer.py)

**Yêu cầu:**

- Gửi sự kiện với các trường:
  - user_id: ID của người dùng
  - event_type: Loại sự kiện (click, view, purchase)
  - item_id: ID của sản phẩm
  - timestamp: Thời điểm xảy ra sự kiện
- Sử dụng Custom Partitioner để phân loại:
  - click vào partition 0
  - view vào partition 1
  - purchase vào partition 2

**Cách triển khai:**

- Custom Partitioner được triển khai thông qua hàm `event_partitioner`
- Sử dụng event_type làm key để phân loại sự kiện vào các partition
- Mô phỏng sự kiện ngẫu nhiên từ các người dùng khác nhau
- Hiển thị kết quả phân partition cho mỗi sự kiện

### 2. Consumer (user_event_consumer.py)

**Yêu cầu:**

- Đọc và đếm số lượng click, view, purchase mỗi phút
- Ghi log kết quả

**Cách triển khai:**

- Sử dụng threading để tạo một luồng riêng cho báo cáo thống kê
- Đếm số lượng sự kiện theo từng loại trong mỗi phút
- Tính toán tỷ lệ phần trăm cho từng loại sự kiện
- Hiển thị thông tin chi tiết với màu sắc trực quan

## Cách sử dụng

### Khởi động Consumer

Mở một terminal và chạy consumer để nhận và phân tích sự kiện:

```bash
python user_event_consumer.py
```

Consumer sẽ bắt đầu lắng nghe và hiển thị thống kê số lượng sự kiện mỗi phút.

### khởi động Producer

Mở một terminal khác và chạy producer để gửi sự kiện:

```bash
python user_event_producer.py
```

Producer sẽ liên tục tạo và gửi các sự kiện ngẫu nhiên đến Kafka.

## Kết quả

Hệ thống này cho phép:

1. Thu thập sự kiện người dùng theo thời gian thực
2. Phân loại sự kiện vào các partition dựa trên loại sự kiện
3. Theo dõi và phân tích số lượng, tỷ lệ của từng loại sự kiện
4. Trực quan hóa kết quả với màu sắc để dễ dàng theo dõi

Thông qua phân tích này, các đội phát triển sản phẩm có thể hiểu rõ hơn về cách người dùng tương tác với ứng dụng và đưa ra các quyết định kinh doanh dựa trên dữ liệu thực tế.
