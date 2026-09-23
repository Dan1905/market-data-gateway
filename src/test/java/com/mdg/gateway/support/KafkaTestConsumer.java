package com.mdg.gateway.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Reads a topic for assertions.
 *
 * <p>Two deliberate choices:
 *
 * <ul>
 *   <li><b>Manual assignment, not {@code subscribe()}.</b> A subscribing consumer must
 *       complete a group join and rebalance before its first poll returns anything, which
 *       on a cold broker is slow and makes tests fail for reasons unrelated to the code
 *       under test.</li>
 *   <li><b>Tail by default.</b> Integration tests share one broker, so a consumer seeking
 *       to the beginning would also see records produced by every earlier test and any
 *       assertion on record <em>count</em> would depend on execution order. Constructed at
 *       the topic's end, each test observes only what it produced itself.</li>
 * </ul>
 */
public final class KafkaTestConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    /** Positions at the end of the topic: only records produced after construction are seen. */
    public KafkaTestConsumer(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, false);
    }

    public KafkaTestConsumer(String bootstrapServers, String topic, boolean fromBeginning) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        this.consumer = new KafkaConsumer<>(props);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(30));
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new IllegalStateException("Topic " + topic + " has no partitions — was it created?");
        }
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo info : partitionInfos) {
            partitions.add(new TopicPartition(info.topic(), info.partition()));
        }
        consumer.assign(partitions);

        if (fromBeginning) {
            consumer.seekToBeginning(partitions);
        } else {
            consumer.seekToEnd(partitions);
        }
        // seek() is lazy; position() forces the offset lookup now so that records produced
        // immediately after this constructor returns are not missed.
        partitions.forEach(consumer::position);
    }

    /**
     * Polls until at least {@code expected} records have accumulated, or the timeout expires.
     * Records are accumulated across polls, so a slow broker does not lose earlier ones.
     */
    public List<ConsumerRecord<String, String>> awaitAtLeast(int expected, Duration timeout) {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline && collected.size() < expected) {
            consumer.poll(Duration.ofMillis(250)).forEach(collected::add);
        }
        return collected;
    }

    /** Polls for the whole window and returns everything seen — for "nothing was produced" assertions. */
    public List<ConsumerRecord<String, String>> drainFor(Duration window) {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        long deadline = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(250)).forEach(collected::add);
        }
        return collected;
    }

    @Override
    public void close() {
        consumer.close(Duration.ofSeconds(5));
    }
}
