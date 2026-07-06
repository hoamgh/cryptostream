-- ============================================================
--  alerts.sql — Grafana alert rules & dashboard queries
--  Paste các query này vào Grafana Panel → Alert tab
-- ============================================================

-- ────────────────────────────────────────────────────────────
--  ALERT 1: Biến động giá lớn (VWAP thay đổi > 3% so với window trước)
--  Trigger: Grafana alert khi result COUNT(*) > 0
-- ────────────────────────────────────────────────────────────
SELECT
    symbol,
    window_start,
    vwap                                                   AS vwap_current,
    LAG(vwap) OVER (PARTITION BY symbol ORDER BY window_start) AS vwap_prev,
    ROUND(
        ABS(vwap - LAG(vwap) OVER (PARTITION BY symbol ORDER BY window_start))
        / NULLIF(LAG(vwap) OVER (PARTITION BY symbol ORDER BY window_start), 0) * 100
    , 2)                                                   AS pct_change
FROM trade_agg_1m
WHERE window_start > NOW() - INTERVAL '5 minutes'
HAVING ABS(vwap - LAG(vwap) OVER (PARTITION BY symbol ORDER BY window_start))
       / NULLIF(LAG(vwap) OVER (PARTITION BY symbol ORDER BY window_start), 0) * 100 > 3;


-- ────────────────────────────────────────────────────────────
--  ALERT 2: Thanh khoản kém — spread quá rộng (> 0.1%)
--  Trigger: Grafana alert khi result COUNT(*) > 0
-- ────────────────────────────────────────────────────────────
SELECT
    symbol,
    window_start,
    avg_best_ask,
    avg_best_bid,
    ROUND((avg_best_ask - avg_best_bid) / NULLIF(avg_best_bid, 0) * 100, 4) AS spread_pct
FROM depth_agg_30s
WHERE window_start > NOW() - INTERVAL '2 minutes'
  AND (avg_best_ask - avg_best_bid) / NULLIF(avg_best_bid, 0) * 100 > 0.1
ORDER BY spread_pct DESC;


-- ────────────────────────────────────────────────────────────
--  ALERT 3: Pipeline health — không có data mới trong 3 phút
--  Trigger: Grafana alert khi result count = 0
-- ────────────────────────────────────────────────────────────
SELECT COUNT(*) AS recent_rows
FROM trade_agg_1m
WHERE created_at > NOW() - INTERVAL '3 minutes';
-- Nếu = 0 → pipeline bị dừng hoặc producer ngắt kết nối


-- ────────────────────────────────────────────────────────────
--  DASHBOARD: P50 / P95 / P99 End-to-End Latency (1 giờ gần nhất)
--  Dùng cho Grafana panel "Pipeline Latency"
-- ────────────────────────────────────────────────────────────
SELECT * FROM latency_percentiles;

--  Hoặc chi tiết theo thời gian (time series panel):
SELECT
    date_trunc('minute', measured_at)                                            AS time,
    pipeline,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 0) AS p95_ms
FROM pipeline_latency
WHERE measured_at > NOW() - INTERVAL '1 hour'
GROUP BY 1, 2
ORDER BY 1;


-- ────────────────────────────────────────────────────────────
--  DASHBOARD: VWAP time series theo symbol (Grafana line chart)
-- ────────────────────────────────────────────────────────────
SELECT
    window_start                    AS time,
    symbol,
    ROUND(vwap::numeric, 2)         AS vwap,
    total_volume
FROM trade_agg_1m
WHERE window_start > NOW() - INTERVAL '1 hour'
ORDER BY time;


-- ────────────────────────────────────────────────────────────
--  DASHBOARD: Trade volume per symbol (bar chart)
-- ────────────────────────────────────────────────────────────
SELECT
    symbol,
    SUM(total_volume)               AS total_vol,
    SUM(trade_count)                AS total_trades,
    ROUND(AVG(vwap)::numeric, 2)    AS avg_price
FROM trade_agg_1m
WHERE window_start > NOW() - INTERVAL '1 hour'
GROUP BY symbol
ORDER BY total_vol DESC;


-- ────────────────────────────────────────────────────────────
--  DASHBOARD: Bid-Ask spread time series (Grafana)
-- ────────────────────────────────────────────────────────────
SELECT
    window_start                                                             AS time,
    symbol,
    ROUND((avg_best_ask - avg_best_bid)::numeric, 4)                         AS spread_abs,
    ROUND((avg_best_ask - avg_best_bid) / NULLIF(avg_best_bid, 0) * 100, 4)  AS spread_pct
FROM depth_agg_30s
WHERE window_start > NOW() - INTERVAL '30 minutes'
ORDER BY time;
