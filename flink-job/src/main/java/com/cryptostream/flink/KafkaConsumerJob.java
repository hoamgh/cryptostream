package com.cryptostream.flink;

import com.cryptostream.flink.model.DepthAgg;
import com.cryptostream.flink.model.KlineRecord;
import com.cryptostream.flink.model.TradeAgg;
import com.cryptostream.flink.process.DepthWindowFunction;
import com.cryptostream.flink.process.TradeWindowFunction;
import com.cryptostream.flink.sink.DepthAggSink;
import com.cryptostream.flink.sink.KlineSink;
import com.cryptostream.flink.sink.TradeAggSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * KafkaConsumerJob — Flink streaming job đọc 3 topic từ Kafka, áp dụng windowing
 * và ghi kết quả vào PostgreSQL.
 *
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │  STREAM         │  WINDOW                        │  OUTPUT TABLE           │
 * ├─────────────────┼────────────────────────────────┼─────────────────────────┤
 * │  crypto-trades  │  Tumbling Event-Time 1 phút    │  trade_agg_1m           │
 * │  crypto-klines  │  Không window (filter đóng nến)│  klines_1m              │
 * │  crypto-depth   │  Sliding Event-Time 30s/10s    │  depth_agg_30s          │
 * └─────────────────────────────────────────────────────────────────────────────┘
 *
 */
public class KafkaConsumerJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerJob.class);

    // ===== Kafka config =====
    private static final String KAFKA_BROKER = "kafka:9092";
    private static final String GROUP_ID     = "flink-consumer-group";

    // ===== Topic names =====
    private static final String TOPIC_TRADES = "crypto-trades";
    private static final String TOPIC_KLINES = "crypto-klines";
    private static final String TOPIC_DEPTH  = "crypto-depth";

    // ===== PostgreSQL config — đọc từ env var (set bởi docker-compose) =====
    private static final String JDBC_URL  = System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://postgres:5432/crypto");
    private static final String DB_USER   = System.getenv("DB_USER");
    private static final String DB_PASS   = System.getenv("DB_PASS");

    // ===== Parallelism =====
    private static final int PARALLELISM = 3;

    // Jackson ObjectMapper — thread-safe khi dùng static final
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // ── 1. Khởi tạo Flink execution environment ──────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(10_000);   // tối đa mất 10s data khi crash
        env.getConfig().setLatencyTrackingInterval(5_000); // Flink latency metrics mỗi 5s

        // ── 2. KafkaSource cho từng topic ────────────────────────────────────
        KafkaSource<String> tradesSource = buildKafkaSource(TOPIC_TRADES);
        KafkaSource<String> klinesSource = buildKafkaSource(TOPIC_KLINES);
        KafkaSource<String> depthSource  = buildKafkaSource(TOPIC_DEPTH);

        // ── 3. WatermarkStrategy (Event-Time) ────────────────────────────────
        //    forBoundedOutOfOrderness(10s): chờ tối đa 10s cho event đến muộn
        //    withIdleness(30s): tránh partition idle làm block watermark toàn job
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withIdleness(Duration.ofSeconds(30));

        // ── 4. DataStream từ Kafka ────────────────────────────────────────────
        DataStream<String> tradesStream = env
                .fromSource(tradesSource, watermarkStrategy, "Kafka-Trades")
                .setParallelism(PARALLELISM)
                .name("trades-source");

        DataStream<String> klinesStream = env
                .fromSource(klinesSource, watermarkStrategy, "Kafka-Klines")
                .setParallelism(PARALLELISM)
                .name("klines-source");

        DataStream<String> depthStream = env
                .fromSource(depthSource, watermarkStrategy, "Kafka-Depth")
                .setParallelism(PARALLELISM)
                .name("depth-source");

        // ═══════════════════════════════════════════════════════════════════════
        //  PIPELINE A — TRADES: Tumbling Window 1 phút → trade_agg_1m
        // ═══════════════════════════════════════════════════════════════════════
        // parse JSON → (symbol, [price, qty]) → keyBy symbol → window → aggregate
        SingleOutputStreamOperator<TradeAgg> tradeAggStream = tradesStream
                .flatMap((String msg, org.apache.flink.util.Collector<org.apache.flink.api.java.tuple.Tuple2<String, double[]>> out) -> {
                    try {
                        JsonNode node   = MAPPER.readTree(msg);
                        String   symbol = node.path("s").asText();
                        double   price  = node.path("p").asDouble();
                        double   qty    = node.path("q").asDouble();
                        if (!symbol.isEmpty() && price > 0 && qty > 0) {
                            out.collect(org.apache.flink.api.java.tuple.Tuple2.of(symbol, new double[]{price, qty}));
                        }
                    } catch (Exception e) {
                        LOG.warn("[trades] Parse error: {}", msg.length() > 120 ? msg.substring(0, 120) : msg, e);
                    }
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.PRIMITIVE_ARRAY(
                                org.apache.flink.api.common.typeinfo.Types.DOUBLE)))
                .name("parse-trades")
                // Watermark đã gắn trên raw stream; sau flatMap vẫn dùng processing-time vì
                // cần gán lại timestamp từ field "T" (trade time) nếu muốn event-time chính xác.
                // Ở đây dùng ingestion-time (watermark từ Kafka source) là đủ cho demo.
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new TradeWindowFunction())
                .name("window-trades-1m");

        tradeAggStream
                .addSink(TradeAggSink.build(JDBC_URL, DB_USER, DB_PASS))
                .name("sink-trade-agg-postgres")
                .setParallelism(PARALLELISM);

        // Log ra console để debug (có thể bỏ ở production)
        tradeAggStream.print().name("print-trade-agg");

        // ═══════════════════════════════════════════════════════════════════════
        //  PIPELINE B — KLINES: Filter nến đóng → persist thẳng → klines_1m
        // ═══════════════════════════════════════════════════════════════════════
        // Binance kline event có field "k.x" = true khi nến đóng.
        // Chỉ lưu nến đóng để tránh ghi partial candle mỗi 250ms.
        SingleOutputStreamOperator<KlineRecord> klineStream = klinesStream
                .flatMap((String msg, org.apache.flink.util.Collector<KlineRecord> out) -> {
                    try {
                        JsonNode node = MAPPER.readTree(msg);
                        JsonNode k    = node.path("k");
                        boolean closed = k.path("x").asBoolean(false);
                        if (!closed) return;

                        String symbol = k.path("s").asText();
                        out.collect(new KlineRecord(
                                symbol,
                                k.path("t").asLong(),
                                k.path("T").asLong(),
                                k.path("o").asDouble(),
                                k.path("h").asDouble(),
                                k.path("l").asDouble(),
                                k.path("c").asDouble(),
                                k.path("v").asDouble(),
                                k.path("n").asLong()
                        ));
                    } catch (Exception e) {
                        LOG.warn("[klines] Parse error: {}", msg.length() > 120 ? msg.substring(0, 120) : msg, e);
                    }
                })
                .returns(KlineRecord.class)
                .name("parse-klines");

        klineStream
                .addSink(KlineSink.build(JDBC_URL, DB_USER, DB_PASS))
                .name("sink-klines-postgres")
                .setParallelism(PARALLELISM);

        klineStream.print().name("print-klines");

        // ═══════════════════════════════════════════════════════════════════════
        //  PIPELINE C — DEPTH: Sliding Window 30s/10s → depth_agg_30s
        // ═══════════════════════════════════════════════════════════════════════
        // depth20@100ms cập nhật mỗi 100ms → ~300 event/phút/symbol.
        // Sliding 30s / slide 10s: mỗi 10s emit 1 record tổng hợp 30s gần nhất.
        SingleOutputStreamOperator<DepthAgg> depthAggStream = depthStream
                .flatMap((String msg, org.apache.flink.util.Collector<org.apache.flink.api.java.tuple.Tuple2<String, double[]>> out) -> {
                    try {
                        JsonNode node    = MAPPER.readTree(msg);
                        String   symbol  = node.path("s").asText();
                        JsonNode bids    = node.path("bids");
                        JsonNode asks    = node.path("asks");
                        if (bids.isEmpty() || asks.isEmpty()) return;

                        double bestBid = bids.get(0).get(0).asDouble();
                        double bestAsk = asks.get(0).get(0).asDouble();
                        if (!symbol.isEmpty() && bestBid > 0 && bestAsk > 0) {
                            out.collect(org.apache.flink.api.java.tuple.Tuple2.of(symbol, new double[]{bestBid, bestAsk}));
                        }
                    } catch (Exception e) {
                        LOG.warn("[depth] Parse error: {}", msg.length() > 120 ? msg.substring(0, 120) : msg, e);
                    }
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.PRIMITIVE_ARRAY(
                                org.apache.flink.api.common.typeinfo.Types.DOUBLE)))
                .name("parse-depth")
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .process(new DepthWindowFunction())
                .name("window-depth-30s");

        depthAggStream
                .addSink(DepthAggSink.build(JDBC_URL, DB_USER, DB_PASS))
                .name("sink-depth-agg-postgres")
                .setParallelism(PARALLELISM);

        depthAggStream.print().name("print-depth-agg");

        // ── 5. Submit job ─────────────────────────────────────────────────────
        env.execute("CryptoStream — Kafka → Window → PostgreSQL");
    }

    // ─────────────────────────────────────────────────────────────────────────
    private static KafkaSource<String> buildKafkaSource(String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setTopics(topic)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
