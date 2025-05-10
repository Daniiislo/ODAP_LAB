import random
from datetime import datetime, timedelta

# Các biến giả lập
ips = ["192.168.1." + str(i) for i in range(1, 50)]
pages = ["/", "/index.html", "/about", "/contact", "/products", "/cart", "/login"]
methods = ["GET", "POST"]
status_codes = [200, 404, 500, 301]
user_agents = ["Mozilla", "Chrome", "Safari", "Edge"]

def generate_log_entry():
    ip = random.choice(ips)
    now = datetime.utcnow() - timedelta(seconds=random.randint(0, 100000))
    time_str = now.strftime('%d/%b/%Y:%H:%M:%S +0000')
    method = random.choice(methods)
    page = random.choice(pages)
    status = random.choice(status_codes)
    size = random.randint(200, 5000)
    return f'{ip} - - [{time_str}] "{method} {page} HTTP/1.1" {status} {size}'

# Tạo nhiều file log
for i in range(1, 6):  # tạo 5 file log
    with open(f'fake_log_{i}.log', 'w') as f:
        for _ in range(1000):  # mỗi file 1000 dòng log
            f.write(generate_log_entry() + '\n')

print("Đã tạo xong các file fake_log_*.log")
