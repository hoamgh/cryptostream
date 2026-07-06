# Crypto Anomaly Detection — Real-time Streaming Pipeline

Hệ thống phát hiện biến động bất thường giá cryptocurrency theo thời gian thực, xây dựng trên nền tảng **Apache Flink** và **Apache Kafka** với kiến trúc Lambda Architecture.

---

## Mục tiêu

Phát hiện ngay lập tức khi giá cryptocurrency (BTC, ETH...) biến động bất thường trong khoảng thời gian ngắn (ví dụ: tăng/giảm >5% trong 1 phút), gửi cảnh báo lên dashboard real-time và lưu trữ toàn bộ lịch sử để phân tích sau.

---

## Kiến trúc tổng thể

```
Binance WebSocket (giá thật, real-time)
        ↓
Python Producer (validate + gửi Kafka)
        ↓
Apache Kafka (vận chuyển, lưu event log)
        ↓
Apache Flink (xử lý stream, Window, Watermark, State)
        ↓
        ├── PostgreSQL (cảnh báo real-time)
        │       ↓
        │   Grafana Dashboard (hiển thị, refresh 1 giây)
        │
        └── MinIO + Apache Iceberg (Lakehouse, lưu lịch sử)
                ↓
            Trino (phân tích xu hướng dài hạn)
```

---

## Tại sao chọn Flink thay vì Spark Structured Streaming?

Đây là quyết định kỹ thuật quan trọng nhất của project.

**Spark Structured Streaming** dùng kiến trúc micro-batch — dữ liệu được gộp thành các lô nhỏ theo Trigger Interval (ví dụ mỗi 1-5 giây) rồi xử lý cả lô cùng lúc. Mỗi batch phải lặp lại chu trình: hỏi Kafka offset mới → tạo Physical Plan → phân task cho Executor → xử lý → ghi kết quả. Overhead này lặp lại liên tục, gây độ trễ tối thiểu vài trăm milliseconds mỗi batch.

**Apache Flink** xử lý true streaming — mỗi event được xử lý ngay khi đến, không cần gộp batch. DAG chỉ được tạo một lần khi job khởi động, TaskManager giữ pipeline chạy liên tục, event chỉ việc chảy qua. Độ trễ đầu cuối ở mức milliseconds.

Với bài toán phát hiện biến động giá crypto — nơi mỗi giây đều có thể xảy ra biến động lớn — độ trễ milliseconds của Flink phù hợp hơn nhiều so với vài trăm milliseconds của Spark.

---

## Tại sao dùng cả PostgreSQL lẫn Lakehouse?

Hai hệ thống này phục vụ hai mục đích hoàn toàn khác nhau.

**PostgreSQL** được tối ưu cho truy vấn đơn lẻ, nhanh — có Index B-Tree giúp tìm 10 cảnh báo mới nhất trong 1-5ms. Grafana refresh dashboard mỗi giây cần độ trễ đọc thấp như vậy.

**Lakehouse (MinIO + Iceberg)** lưu toàn bộ lịch sử giá gốc theo định dạng Parquet, partition theo ngày/giờ. Tối ưu cho việc đọc hàng loạt và phân tích phức tạp (tổng hợp theo tuần, tháng, train ML model). Truy vấn đơn lẻ "10 dòng mới nhất" sẽ chậm hơn PostgreSQL do phải mở file Parquet, nhưng query "tổng hợp 6 tháng" sẽ nhanh hơn nhiều.

Đây là pattern **Lambda Architecture** — Speed Layer (PostgreSQL) cho phản hồi nhanh, Batch Layer (Lakehouse) cho phân tích sâu.

---

## Tech Stack

| Thành phần      | Công nghệ                     | Vai trò                        |
| ----------------- | ------------------------------- | ------------------------------- |
| Nguồn dữ liệu  | Binance WebSocket API           | Giá crypto real-time           |
| Message Queue     | Apache Kafka                    | Vận chuyển event, lưu log    |
| Stream Processing | Apache Flink 1.18 (Java)        | Xử lý, phát hiện anomaly    |
| Speed Layer DB    | PostgreSQL 15                   | Lưu cảnh báo real-time       |
| Batch Layer       | MinIO + Apache Iceberg          | Lưu lịch sử dài hạn        |
| Query Engine      | Trino                           | Phân tích dữ liệu lớn      |
| Dashboard         | Grafana                         | Hiển thị cảnh báo real-time |
| Container         | Docker + Docker Compose         | Orchestration toàn bộ         |
| Language          | Python (Producer), Java (Flink) |                                 |

---

## Cấu trúc thư mục

```
crypto-anomaly-flink/
├── docker-compose.yml          # Toàn bộ hạ tầng
├── .env                        # Config nhạy cảm (không commit)
│
├── producer/
│   ├── binance_ws.py           # WebSocket đọc giá Binance
│   ├── validator.py            # Validate trước khi gửi Kafka
│   └── requirements.txt
│
├── flink-job/
│   ├── pom.xml                 # Maven dependencies
│   └── src/main/java/
│       ├── KafkaConsumerJob.java
│       ├── AnomalyDetector.java    # Logic phát hiện bất thường
│       ├── PostgresSink.java       # Ghi cảnh báo vào PostgreSQL
│       └── IcebergSink.java        # Ghi lịch sử vào Lakehouse
│
├── tests/
│   ├── test_validator.py       # Test logic validate Producer
│   └── AnomalyDetectorTest.java # Test logic Flink
│
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       └── dashboard.json
│
├── sql/
│   └── schema.sql              # PostgreSQL schema
│
├── .github/workflows/
│   └── ci.yml                  # GitHub Actions CI/CD
│
└── README.md
```

---

## Các khái niệm kỹ thuật quan trọng

### Window và Watermark

Flink dùng **Tumbling Window 1 phút** để tính % biến động giá trong từng khoảng thời gian. Mỗi event mang Event Time (timestamp từ Binance), Flink gắn event vào đúng Window theo thời gian đó.

**Watermark** quyết định khi nào đóng một Window — được đặt là 10 giây, nghĩa là Flink chờ tối đa 10 giây cho các event đến trễ trước khi chốt kết quả. Event đến sau 10 giây được gửi vào Dead Letter Queue thay vì bỏ qua hoàn toàn.

### Fault Tolerance

Flink Checkpoint được kích hoạt mỗi 30 giây — snapshot trạng thái toàn bộ pipeline vào MinIO. Nếu job crash, Flink tự phục hồi từ checkpoint gần nhất, đảm bảo exactly-once processing — không mất dữ liệu, không xử lý trùng.

### Data Validation

Validate được thực hiện ở 2 tầng với mục đích khác nhau. Producer validate dữ liệu đơn giản (price > 0, symbol không null, JSON hợp lệ) để giữ Kafka sạch — Kafka là "source of truth", không nên có dữ liệu rác. Flink validate phức tạp hơn (so sánh với giá lịch sử, phát hiện pattern) vì cần State và Window mà Producer không có.

---

## Chạy project

### Yêu cầu

- Docker Desktop (WSL2 backend)
- Python 3.10+
- Java 17 (Temurin)
- Maven 3.8+

### Khởi động hạ tầng

```bash
docker-compose up -d
docker ps
```

Kiểm tra các service:

- Kafka UI: http://localhost:8080
- Flink Dashboard: http://localhost:8081
- Grafana: http://localhost:3000 (admin/admin)

### Chạy Producer

```bash
cd producer
pip install -r requirements.txt
python binance_ws.py
```

### Build và submit Flink Job

```bash
cd flink-job
mvn clean package
docker cp target/flink-job-1.0.jar flink-jobmanager:/opt/flink/usrlib/
docker exec flink-jobmanager flink run /opt/flink/usrlib/flink-job-1.0.jar
```

---

## Monitoring và Observability

- **Consumer Lag**: theo dõi qua Kafka UI hoặc Prometheus — nếu lag tăng liên tục, Flink đang không đọc kịp dữ liệu từ Kafka
- **Flink Dashboard**: xem throughput, checkpoint status, backpressure tại http://localhost:8081
- **Dead Letter Queue**: topic `crypto-prices-dlq` chứa các message lỗi để debug
- **Grafana**: dashboard hiển thị số cảnh báo theo thời gian, refresh mỗi 1 giây

---

## Hướng mở rộng (Future improvements)

- Thêm Schema Registry (Confluent) để enforce data contract giữa Producer và Flink
- Tích hợp Great Expectations cho data quality check tự động
- Thêm Trino để query phân tích lịch sử từ Iceberg Lakehouse
- Train model ML dự đoán biến động giá từ dữ liệu lịch sử trong Lakehouse
- Scale Flink TaskManager theo load thực tế (horizontal scaling)
- Thêm Airflow cho batch pipeline song song (báo cáo ngày, tuần)

---

## Project này được xây dựng để thực hành và thể hiện kỹ năng Data Engineering thực tế, bao gồm stream processing, distributed systems, và data quality engineering.
