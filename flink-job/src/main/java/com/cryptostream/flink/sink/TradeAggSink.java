package com.cryptostream.flink.sink;

import com.cryptostream.flink.model.TradeAgg;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Sink ghi TradeAgg vào bảng trade_agg_1m trong PostgreSQL.
 *
 * Dùng upsert (INSERT ... ON CONFLICT DO UPDATE) để an toàn khi Flink
 * retry sau checkpoint failure — tránh duplicate rows.
 *
 * Batch: 100 records hoặc sau 2 giây, tùy cái nào đến trước.
 */
public class TradeAggSink {

    private static final String UPSERT_SQL = """
            INSERT INTO trade_agg_1m
                (symbol, window_start, window_end, trade_count, total_volume, vwap, min_price, max_price)
            VALUES (?, to_timestamp(? / 1000.0), to_timestamp(? / 1000.0), ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, window_start) DO UPDATE SET
                window_end   = EXCLUDED.window_end,
                trade_count  = EXCLUDED.trade_count,
                total_volume = EXCLUDED.total_volume,
                vwap         = EXCLUDED.vwap,
                min_price    = EXCLUDED.min_price,
                max_price    = EXCLUDED.max_price
            """;

    public static SinkFunction<TradeAgg> build(String jdbcUrl, String user, String password) {
        return JdbcSink.sink(
                UPSERT_SQL,
                (stmt, agg) -> {
                    stmt.setString(1, agg.symbol);
                    stmt.setLong  (2, agg.windowStart);
                    stmt.setLong  (3, agg.windowEnd);
                    stmt.setLong  (4, agg.tradeCount);
                    stmt.setDouble(5, agg.totalVolume);
                    stmt.setDouble(6, agg.vwap);
                    stmt.setDouble(7, agg.minPrice);
                    stmt.setDouble(8, agg.maxPrice);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(500)   // giảm từ 2s → 500ms, giảm latency
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        );
    }
}
