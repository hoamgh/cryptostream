-- ============================================================
--  CryptoStream — PostgreSQL Schema
--  Chạy script này một lần trước khi start Flink job.
--  Kết nối: psql -h localhost -U hoang -d crypto -f init.sql
-- ============================================================

-- ────────────────────────────────────────────────────────────
--  1. trade_agg_1m
--     Kết quả Tumbling Window 1 phút từ stream crypto-trades.
--     Lưu VWAP, volume, min/max price mỗi phút, mỗi symbol.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_agg_1m (
    symbol       VARCHAR(20)      NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    trade_count  BIGINT           NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    vwap         DOUBLE PRECISION NOT NULL,
    min_price    DOUBLE PRECISION NOT NULL,
    max_price    DOUBLE PRECISION NOT NULL,
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_trade_agg_symbol_time
    ON trade_agg_1m (symbol, window_start DESC);

COMMENT ON TABLE trade_agg_1m IS
    'VWAP + volume tổng hợp mỗi 1 phút per symbol từ Flink Tumbling Window';

-- ────────────────────────────────────────────────────────────
--  2. klines_1m
--     Nến OHLCV 1 phút đã đóng từ stream crypto-klines.
--     Binance đã tổng hợp sẵn, Flink chỉ filter và persist.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS klines_1m (
    symbol      VARCHAR(20)      NOT NULL,
    open_time   TIMESTAMPTZ      NOT NULL,
    close_time  TIMESTAMPTZ      NOT NULL,
    open_price  DOUBLE PRECISION NOT NULL,
    high_price  DOUBLE PRECISION NOT NULL,
    low_price   DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    num_trades  BIGINT           NOT NULL,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_klines_symbol_time
    ON klines_1m (symbol, open_time DESC);

COMMENT ON TABLE klines_1m IS
    'Nến OHLCV 1 phút đã đóng từ Binance kline stream, persist bởi Flink';

-- ────────────────────────────────────────────────────────────
--  3. depth_agg_30s
--     Kết quả Sliding Window 30s (slide 10s) từ crypto-depth.
--     Spread trung bình và bid/ask trung bình mỗi 10 giây.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS depth_agg_30s (
    symbol         VARCHAR(20)      NOT NULL,
    window_start   TIMESTAMPTZ      NOT NULL,
    window_end     TIMESTAMPTZ      NOT NULL,
    avg_spread     DOUBLE PRECISION NOT NULL,
    avg_best_bid   DOUBLE PRECISION NOT NULL,
    avg_best_ask   DOUBLE PRECISION NOT NULL,
    snapshot_count BIGINT           NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_depth_agg_symbol_time
    ON depth_agg_30s (symbol, window_start DESC);

COMMENT ON TABLE depth_agg_30s IS
    'Spread + bid/ask trung bình mỗi 30s (slide 10s) từ Flink Sliding Window';

-- ────────────────────────────────────────────────────────────
--  4. pipeline_latency
--     Bảng đo latency end-to-end: event_time (Binance) → DB write.
--     Dùng để tính P50/P95/P99 latency của mỗi pipeline.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_latency (
    id           BIGSERIAL        PRIMARY KEY,
    measured_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    pipeline     VARCHAR(20)      NOT NULL,     -- 'trades', 'klines', 'depth'
    symbol       VARCHAR(20)      NOT NULL,
    event_time   TIMESTAMPTZ      NOT NULL,     -- timestamp gốc từ Binance
    latency_ms   DOUBLE PRECISION NOT NULL      -- = extract(epoch from measured_at - event_time) * 1000
);

CREATE INDEX IF NOT EXISTS idx_latency_pipeline_time
    ON pipeline_latency (pipeline, measured_at DESC);

COMMENT ON TABLE pipeline_latency IS
    'Đo latency end-to-end từ event Binance đến khi ghi DB. Dùng PERCENTILE_CONT để tính P95.';

-- ────────────────────────────────────────────────────────────
--  View tiện: P50/P95/P99 latency theo pipeline (1 giờ gần nhất)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW latency_percentiles AS
SELECT
    pipeline,
    COUNT(*)                                                              AS sample_count,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::numeric, 0) AS p50_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 0) AS p95_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric, 0) AS p99_ms,
    ROUND(AVG(latency_ms)::numeric, 0)                                   AS avg_ms
FROM pipeline_latency
WHERE measured_at > NOW() - INTERVAL '1 hour'
GROUP BY pipeline;

-- ────────────────────────────────────────────────────────────
--  Quick-check: xem các bảng vừa tạo
-- ────────────────────────────────────────────────────────────
SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('trade_agg_1m', 'klines_1m', 'depth_agg_30s', 'pipeline_latency')
ORDER BY table_name;
