# 1.Tạo folder /logs/2025/03 trên HDFS.

`hdfs dfs -mkdir -p /logs/2025/03`
Tham số `-p` là viết tắt của `parents`. Khi sử dụng tham só này, HDFS sẽ tự động tạo các thư mục cha cần thiết nếu chúng chưa tồn tại.

# 2. Upload các file log giả lập vào đó.

`hdfs dfs -put ~/fake-logs/* /logs/2025/03/`

# 3. Kiểm tra dung lượng đã sử dụng trên HDFS.

`hdfs dfs -du -h /logs/2025/03`
Tham số `-du` là viết tắt của `disk usage` giúp hiển thị dung lượng sử dụng.

Tham số `-h` là viết tắt của `human-readable`, nghĩa là dung lượng theo định dạng dễ đọc hơn (KB, MB, GB...) thay vì số byte thuần túy.

# 4. Kiểm tra quyền truy cập của các file (xem ai có quyền đọc, ghi, thực thi).

`hdfs dfs -ls /logs/2025/03`

Lệnh này sẽ hiển thị quyền truy cập (rwx), chủ sở hữu (owner), nhóm (group).

Ví dụ đầu ra:

`-rw-r--r--   3 hadoop hadoopusr   1048576 2025-05-10 10:00 /logs/2025/03/log1.txt`

# 5. Sử dụng lệnh du và dfsadmin -report để kiểm tra dung lượng từng thư mục và tình trạng của cụm Hadoop.

Dùng lệnh `du`:

`hdfs dfs -du -h /logs/2025`

Dùng lệnh `dfsadmin -report`:

`hdfs dfsadmin -report`

Lệnh này cung cấp thông tin chi tiết về dung lượng còn trống, đã dùng, số lượng DataNode đang online/offline,...

# 6. Giả lập tình huống có một file rất lớn (trên 1GB)

**Yêu cầu**:

- Chia file thành các phần nhỏ (split).
- Tải từng phần lên HDFS và hợp nhất lại.
- Kiểm tra sự phân phối các block của file trên các DataNode bằng lệnh hdfs fsck.

## a. Chia nhỏ file lớn (split):

Giả sử file big_log.txt lớn hơn 1GB:

`split -b 256M big_log.txt part_`

File sẽ được chia thành các phần part_aa, part_ab,...

## b. Upload từng phần lên HDFS

`hdfs dfs -mkdir /logs/2025/03/bigfile_parts`

`hdfs dfs -put part_* /logs/2025/03/bigfile_parts/`

## c. Hợp nhất các phần lại trong HDFS

`hdfs dfs -cat /logs/2025/03/bigfile_parts/part_* | hdfs dfs -put - /logs/2025/03/big_log_combined.txt`

## d. Kiểm tra phân phối block của file

`hdfs fsck /logs/2025/03/big_log_combined.txt -files -blocks -locations`

Lệnh trên hiển thị các block của file và DataNode tương ứng đang lưu trữ block đó.

# 7. Giả sử một DataNode bị lỗi, hãy tìm hiểu cách Hadoop đảm bảo dữ liệu không bị mất

Nếu một DataNode bị lỗi, Hadoop đảm bảo an toàn dữ liệu như thế nào?

Hadoop sử dụng cơ chế replication: Mỗi block của file sẽ được sao chép lên n bản trên các DataNode khác nhau.

Khi một DataNode bị lỗi:

- NameNode phát hiện lỗi qua việc không nhận heartbeat.

- Hadoop sẽ tự động replicate các block bị mất sang DataNode còn sống để đảm bảo số lượng bản sao luôn đủ.

- Dữ liệu không bị mất miễn là có ít nhất 1 bản sao còn tồn tại.

Kiểm tra replication của file bằng:

`hdfs fsck /logs/2025/03/big_log_combined.txt -files -blocks -locations`

Kết quả:

```
Status: HEALTHY
 Total size:	1200000000 B
 Total dirs:	0
 Total files:	1
 Total symlinks:		0
 Total blocks (validated):	9 (avg. block size 133333333 B)
 Minimally replicated blocks:	9 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	1
 Average block replication:	1.0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)
 Number of data-nodes:		1
 Number of racks:		1
FSCK ended at Sat May 10 21:26:00 ICT 2025 in 1 milliseconds
```

Ở đây, file big_log_file_combined.txt chỉ có 1 bản sao(`Default replication factor:	1`), do đó nếu DataNode duy nhất bị lỗi, dữ liệu sẽ bị mất

Để tăng số replication factor, ta chỉnh cấu hình HDFS (hdfs-site.xml)

```xml
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
```

Hoặc có thể thay đổi trên 1 file cụ thể bằng lệnh

`hdfs dfs -setrep -R 3 <file_path>`
