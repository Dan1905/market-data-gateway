package com.mdg.gateway.service;

import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.dto.DlqReplayRequest;
import com.mdg.gateway.dto.DlqReplayResponse;
import com.mdg.gateway.exception.DlqReplayException;
import com.mdg.gateway.exception.ReplayAlreadyRunningException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DlqHeaders;
import com.mdg.gateway.producer.MarketDataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Drains {@code market-data-dlq} and re-runs each record through the normal pipeline.
 *
 * <h2>Why a bare KafkaConsumer and not a listener container</h2>
 * Replay is a bounded, operator-triggered batch job. A {@code @KafkaListener} would hold a
 * consumer-group slot forever, consume records the instant they are dead-lettered (before
 * anyone has fixed the cause), and race the HTTP call for the same offsets. Here the
 * consumer is constructed per run with a throwaway group id, manually assigned, seeked to
 * the beginning, drained, and closed.
 *
 * <h2>Offsets are deliberately never committed</h2>
 * A replay is a <em>read</em> of the DLQ, not a consumption of it. Records stay in place
 * until their retention expires, so a fix-and-replay cycle can be run repeatedly and an
 * operator can always see what is still broken. The cost is that replay always re-reads
 * from the beginning — which is also why {@code maxRecords} and the duration budget exist.
 *
 * <h2>Single-flight</h2>
 * Two concurrent replays would read the same offsets and publish every event twice, so the
 * second caller is rejected with 409 rather than queued.
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

    private final Properties consumerProperties;
    private final PayloadTransformationService transformationService;
    private final MarketDataProducer producer;
    private final GatewayProperties properties;

    private final ReentrantLock replayLock = new ReentrantLock();

    public DlqReplayService(Properties dlqReplayConsumerProperties,
                            PayloadTransformationService transformationService,
                            MarketDataProducer producer,
                            GatewayProperties properties) {
        this.consumerProperties = dlqReplayConsumerProperties;
        this.transformationService = transformationService;
        this.producer = producer;
        this.properties = properties;
    }

    public DlqReplayResponse replay(DlqReplayRequest request) {
        DlqReplayRequest effective = request == null ? DlqReplayRequest.defaults() : request;

        if (!replayLock.tryLock()) {
            throw new ReplayAlreadyRunningException(
                    "A DLQ replay is already in progress; wait for it to finish before starting another");
        }
        try {
            return doReplay(effective);
        } finally {
            replayLock.unlock();
        }
    }

    private DlqReplayResponse doReplay(DlqReplayRequest request) {
        Instant startedAt = Instant.now();
        long deadlineNanos = System.nanoTime() + properties.dlq().replayMaxDuration().toNanos();

        Optional<Exchange> filter = Optional.ofNullable(request.exchangeFilter())
                .filter(value -> !value.isBlank())
                .map(value -> Exchange.fromName(value).orElseThrow(() -> new DlqReplayException(
                        "Unknown exchangeFilter '" + value + "'; expected one of BINANCE, COINBASE, KRAKEN")));

        String topic = properties.topics().deadLetter();
        int consumed = 0;
        int filtered = 0;
        int republished = 0;
        int stillFailing = 0;
        Set<String> failureSamples = new LinkedHashSet<>();

        Properties runProperties = new Properties();
        runProperties.putAll(consumerProperties);
        runProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                properties.dlq().replayGroupPrefix() + "-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(runProperties)) {

            List<TopicPartition> partitions = assignAllPartitions(consumer, topic);
            if (partitions.isEmpty()) {
                log.info("DLQ replay: topic {} has no partitions — nothing to replay", topic);
                return summary(startedAt, request, 0, 0, 0, 0, List.of());
            }
            consumer.seekToBeginning(partitions);

            Duration pollTimeout = Duration.ofMillis(request.pollTimeoutMsOrDefault());
            int maxRecords = request.maxRecordsOrDefault();
            int emptyPolls = 0;

            while (consumed < maxRecords
                    && emptyPolls < request.emptyPollsBeforeStopOrDefault()
                    && System.nanoTime() < deadlineNanos) {

                ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    if (consumed >= maxRecords) {
                        break;
                    }
                    consumed++;

                    Exchange exchange = resolveExchange(record);
                    if (filter.isPresent() && filter.get() != exchange) {
                        filtered++;
                        continue;
                    }
                    if (exchange == null) {
                        stillFailing++;
                        failureSamples.add("offset " + record.offset() + ": unresolvable source exchange");
                        continue;
                    }

                    try {
                        List<CanonicalTradeEvent> events =
                                transformationService.transform(exchange, record.value());

                        if (events.isEmpty()) {
                            // The original failure was a control frame mis-classified as a
                            // failure, or a since-fixed filter. Nothing to publish; not an error.
                            continue;
                        }
                        if (request.dryRunOrDefault()) {
                            republished += events.size();
                            continue;
                        }
                        for (CanonicalTradeEvent event : events) {
                            producer.publishOrThrow(event);
                            republished++;
                        }
                    } catch (RuntimeException ex) {
                        stillFailing++;
                        if (failureSamples.size() < properties.dlq().failureSampleLimit()) {
                            failureSamples.add("offset " + record.offset() + " (" + exchange + "): "
                                    + rootMessage(ex));
                        }
                        log.debug("DLQ replay still failing at offset {}", record.offset(), ex);
                    }
                }
            }

            if (System.nanoTime() >= deadlineNanos) {
                log.warn("DLQ replay hit the {} duration budget after {} records",
                        properties.dlq().replayMaxDuration(), consumed);
            }

        } catch (ReplayAlreadyRunningException | DlqReplayException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DlqReplayException("DLQ replay failed against topic " + topic, ex);
        }

        DlqReplayResponse response = summary(startedAt, request, consumed, filtered, republished,
                stillFailing, List.copyOf(failureSamples));
        log.info("DLQ replay complete: consumed={} filtered={} republished={} stillFailing={} dryRun={} in {}ms",
                consumed, filtered, republished, stillFailing, request.dryRunOrDefault(), response.durationMs());
        return response;
    }

    // ------------------------------------------------------------------

    private List<TopicPartition> assignAllPartitions(KafkaConsumer<String, String> consumer, String topic) {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            return List.of();
        }
        List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
        for (PartitionInfo info : partitionInfos) {
            partitions.add(new TopicPartition(info.topic(), info.partition()));
        }
        consumer.assign(partitions);
        return partitions;
    }

    /**
     * Reads the source venue from the DLQ header, falling back to the record key.
     *
     * <p>Header values originate from this gateway, but a DLQ is exactly the place where
     * malformed and hand-injected records accumulate, so an unparseable value yields
     * {@code null} and is reported rather than throwing.
     */
    private Exchange resolveExchange(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(DlqHeaders.SOURCE_EXCHANGE);
        if (header != null && header.value() != null) {
            Optional<Exchange> fromHeader =
                    Exchange.fromName(new String(header.value(), StandardCharsets.UTF_8));
            if (fromHeader.isPresent()) {
                return fromHeader.get();
            }
        }
        return Exchange.fromName(record.key()).orElse(null);
    }

    private DlqReplayResponse summary(Instant startedAt, DlqReplayRequest request, int consumed,
                                      int filtered, int republished, int stillFailing,
                                      List<String> failureSamples) {
        return DlqReplayResponse.builder()
                .startedAt(startedAt)
                .durationMs(Duration.between(startedAt, Instant.now()).toMillis())
                .dryRun(request.dryRunOrDefault())
                .consumed(consumed)
                .filtered(filtered)
                .republished(republished)
                .stillFailing(stillFailing)
                .failureSamples(failureSamples)
                .build();
    }

    private static String rootMessage(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
    }
}
