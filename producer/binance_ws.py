import websocket
import json
import time
import os
from confluent_kafka import Producer

SYMBOLS = [
    "BTCUSDT",   # Bitcoin
    "ETHUSDT",   # Ethereum
    "BNBUSDT",   # Binance Coin
    "SOLUSDT",   # Solana
    "XRPUSDT"    # XRP
]

# ==== Cấu hình Kafka Producer ====
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'binance-producer',
    'linger.ms': 10,   # Đợi 10ms để gom nhóm tin nhắn trước khi gửi
    'acks': 1          # Nhận ack từ leader broker là đủ
}

try:
    producer = Producer(conf)
    print("Khởi tạo Kafka Producer thành công!")
except Exception as e:
    print("Lỗi khởi tạo Kafka Producer:", e)
    producer = None


def delivery_report(err, msg):
    """Callback báo trạng thái gửi tin nhắn lên Kafka."""
    if err is not None:
        print(f"Gửi tin nhắn lên Kafka thất bại: {err}")


def get_topic_from_stream_name(stream_name: str) -> str:
    """Map loại stream -> topic Kafka tương ứng."""
    if "@trade" in stream_name:
        return "crypto-trades"
    elif "@kline_" in stream_name:
        return "crypto-klines"
    elif "@depth" in stream_name:
        return "crypto-depth"
    else:
        return "crypto-others"


def on_message(ws, message):
    try:
        payload = json.loads(message)

        # Combined stream luôn có 2 field: "stream" và "data"
        stream_name = payload.get("stream", "")
        data = payload.get("data", {})

        topic = get_topic_from_stream_name(stream_name)

        # Lấy symbol để làm key:
        # - trade/kline: field "s" có sẵn trong data
        # - depth: KHÔNG có field "s" → parse từ stream_name
        symbol = data.get("s") or stream_name.split("@")[0].upper()

        # Với depth event: inject symbol vào payload để Flink đọc được qua field "s"
        if "@depth" in stream_name and "s" not in data:
            data["s"] = symbol

        # Log gọn để kiểm tra
        if "@trade" in stream_name:
            event_time_ms = data.get("T")
            if event_time_ms:
                l1_ms = time.time() * 1000 - event_time_ms
                print(f"[TRADE][{symbol}] price={data.get('p')} qty={data.get('q')} L1={l1_ms:.0f}ms")
            else:
                print(f"[TRADE][{symbol}] price={data.get('p')} qty={data.get('q')}")
        elif "@kline_" in stream_name:
            k = data.get("k", {})
            print(f"[KLINE][{symbol}] close={k.get('c')} vol={k.get('v')} closed={k.get('x')}")
        elif "@depth" in stream_name:
            best_bid = data.get("bids", [[None]])[0][0]
            best_ask = data.get("asks", [[None]])[0][0]
            print(f"[DEPTH][{symbol}] bid={best_bid} ask={best_ask}")

        if producer is not None:
            producer.produce(
                topic=topic,
                value=json.dumps(data),
                key=symbol,
                callback=delivery_report
            )
            producer.poll(0)

    except Exception as e:
        print("Lỗi xử lý dữ liệu:", e)


def on_error(ws, error):
    print("Lỗi kết nối:", error)


def on_close(ws, close_status_code, close_msg):
    print("Đã đóng kết nối với Binance WS.")


def on_open(ws):
    print("Đã mở kết nối với Binance WS thành công!")


def run_with_retry(uri):
    """Chạy WebSocket với auto-reconnect khi bị ngắt kết nối."""
    retry_delay = 5  # giây
    while True:
        try:
            print(f"[WS] Đang kết nối tới Binance...")
            ws = websocket.WebSocketApp(
                uri,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=70, ping_timeout=10)
        except KeyboardInterrupt:
            print("\nĐang dừng kết nối...")
            break
        except Exception as e:
            print(f"[WS] Lỗi: {e}")
        print(f"[WS] Mất kết nối. Thử lại sau {retry_delay}s...")
        time.sleep(retry_delay)


if __name__ == "__main__":
    # Xây danh sách stream cho 3 loại dữ liệu:
    # - trade: từng lệnh khớp real-time
    # - kline_1m: nến 1 phút (OHLCV)
    # - depth20@100ms: top 20 bid/ask, cập nhật mỗi 100ms
    streams = []
    for s in SYMBOLS:
        sym = s.lower()
        streams.append(f"{sym}@trade")
        streams.append(f"{sym}@kline_1m")
        streams.append(f"{sym}@depth20@100ms")

    stream_param = "/".join(streams)
    uri = f"wss://stream.binance.com:9443/stream?streams={stream_param}"
    print(f"Đang khởi động Producer với {len(streams)} stream...")

    try:
        run_with_retry(uri)
    finally:
        if producer is not None:
            print("Đang flush Kafka Producer...")
            producer.flush()