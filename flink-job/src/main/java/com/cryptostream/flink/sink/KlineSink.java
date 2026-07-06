package com.cryptostream.flink.sink;

import com.cryptostream.flink.model.KlineRecord;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Sink ghi KlineRecord (nến đã đóng) vào bảng klines_1m trong PostgreSQL.
 *
 * Chỉ những nến đã đóng (isClosed=true từ producer) mới được đẩy vào stream
 * → không cần lọc thêm ở đây.
 *
 * ON CONFLICT trên (symbol, open_time) để idempotent.
 */
public class KlineSink {

    private static final String UPSERT_SQL = """
            INSERT INTO klines_1m
                (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, num_trades)
            VALUES (?, to_timestamp(? / 1000.0), to_timestamp(? / 1000.0), ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, open_time) DO UPDATE SET
                close_time  = EXCLUDED.close_time,
                open_price  = EXCLUDED.open_price,
                high_price  = EXCLUDED.high_price,
                low_price   = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume      = EXCLUDED.volume,
                num_trades  = EXCLUDED.num_trades
            """;

    public static SinkFunction<KlineRecord> build(String jdbcUrl, String user, String password) {
        return JdbcSink.sink(
                UPSERT_SQL,
                (stmt, k) -> {
                    stmt.setString(1, k.symbol);
                    stmt.setLong  (2, k.openTime);
                    stmt.setLong  (3, k.closeTime);
                    stmt.setDouble(4, k.open);
                    stmt.setDouble(5, k.high);
                    stmt.setDouble(6, k.low);
                    stmt.setDouble(7, k.close);
                    stmt.setDouble(8, k.volume);
                    stmt.setLong  (9, k.numTrades);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(50)
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
