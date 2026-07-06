package com.cryptostream.flink.sink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * DeadLetterQueueSink — ghi message lỗi vào Kafka topic "crypto-dlq".
 *
 * Cách dùng trong KafkaConsumerJob:
 *   OutputTag<String> dlqTag = new OutputTag<String>("dead-letter"){};
 *   ctx.output(dlqTag, rawMsg);   // trong catch block
 *   stream.getSideOutput(dlqTag).sinkTo(DeadLetterQueueSink.build());
 */
public class DeadLetterQueueSink {

    private static final String DLQ_TOPIC   = "crypto-dlq";
    private static final String KAFKA_BROKER = "kafka:9092";

    /**
     * Trả về KafkaSink<String> ghi message lỗi vào topic crypto-dlq.
     * Message format: {"error":"<reason>","raw":"<original_message>","ts":<epoch_ms>}
     */
    public static KafkaSink<String> build() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(DLQ_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }

    /**
     * Tạo DLQ payload JSON từ exception và raw message.
     */
    public static String buildPayload(String rawMsg, Exception e) {
        // Escape quotes đơn giản — không cần thêm dependency Jackson
        String safeRaw  = rawMsg.replace("\\", "\\\\").replace("\"", "\\\"");
        String safeErr  = e.getClass().getSimpleName() + ": " + e.getMessage();
        safeErr = safeErr.replace("\"", "'");
        return String.format(
                "{\"error\":\"%s\",\"raw\":\"%s\",\"ts\":%d}",
                safeErr,
                safeRaw.length() > 500 ? safeRaw.substring(0, 500) : safeRaw,
                System.currentTimeMillis()
        );
    }
}
