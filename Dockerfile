# Sử dụng một hình ảnh Python chính thức làm nền
FROM python:3.9-slim-buster

# Đặt thư mục làm việc bên trong container
WORKDIR /app

# Sao chép tệp requirements.txt vào thư mục làm việc
COPY requirements.txt .

# Cài đặt các thư viện Python cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn ứng dụng vào thư mục làm việc
COPY . .

# Mở cổng mà ứng dụng Flask sẽ lắng nghe
# Cổng này phải khớp với cổng được cấu hình trong main.py (mặc định là 5001)
EXPOSE 5001

# Lệnh để chạy ứng dụng bằng Gunicorn
# 'main:app' có nghĩa là tìm biến 'app' trong tệp 'main.py'
# -b 0.0.0.0:$PORT đảm bảo Gunicorn lắng nghe trên tất cả các giao diện và sử dụng biến môi trường PORT của Fly.io
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5001", "main:app"]
