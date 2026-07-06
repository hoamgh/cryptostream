package com.cryptostream.flink.sink;

import com.cryptostream.flink.model.DepthAgg;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Sink ghi DepthAgg vào bảng depth_agg_30s trong PostgreSQL.
 *
 * Window là Sliding 30s / slide 10s nên mỗi record có overlap với record khác.
 * ON CONFLICT trên (symbol, window_start) để idempotent khi retry.
 */
public class DepthAggSink {

    private static final String UPSERT_SQL = """
            INSERT INTO depth_agg_30s
                (symbol, window_start, window_end, avg_spread, avg_best_bid, avg_best_ask, snapshot_count)
            VALUES (?, to_timestamp(? / 1000.0), to_timestamp(? / 1000.0), ?, ?, ?, ?)
            ON CONFLICT (symbol, window_start) DO UPDATE SET
                window_end      = EXCLUDED.window_end,
                avg_spread      = EXCLUDED.avg_spread,
                avg_best_bid    = EXCLUDED.avg_best_bid,
                avg_best_ask    = EXCLUDED.avg_best_ask,
                snapshot_count  = EXCLUDED.snapshot_count
            """;

    public static SinkFunction<DepthAgg> build(String jdbcUrl, String user, String password) {
        return JdbcSink.sink(
                UPSERT_SQL,
                (stmt, agg) -> {
                    stmt.setString(1, agg.symbol);
                    stmt.setLong  (2, agg.windowStart);
                    stmt.setLong  (3, agg.windowEnd);
                    stmt.setDouble(4, agg.avgSpread);
                    stmt.setDouble(5, agg.avgBestBid);
                    stmt.setDouble(6, agg.avgBestAsk);
                    stmt.setLong  (7, agg.snapshotCount);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(200)
                        .withBatchIntervalMs(500)   // giảm từ 2s → 500ms
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
