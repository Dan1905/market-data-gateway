package com.mdg.gateway.config;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import io.micrometer.core.instrument.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.time.Duration;
import java.util.Set;

/**
 * Sends the few metrics worth being woken up for to AWS CloudWatch, so an alarm can email
 * someone when trades are lost.
 *
 * <h2>Only a handful, on purpose</h2>
 * CloudWatch bills per custom metric, and every distinct name-and-tag combination is its own
 * metric. Exporting everything the gateway measures would be hundreds of them. This exports
 * {@link #EXPORTED} only, with the per-exchange tag folded away, which keeps it inside the free
 * tier's 10 custom metrics. Prometheus still gets everything; this filter applies to the
 * CloudWatch registry alone.
 *
 * <h2>Credentials</h2>
 * None in the application or its configuration. The SDK's default chain picks up the EC2
 * instance's IAM role, which needs {@code cloudwatch:PutMetricData} for this namespace only
 * (see deploy/README.md). A container reaches the instance metadata service through one extra
 * network hop, so the instance's metadata hop limit must be 2.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "gateway.cloudwatch", name = "enabled", havingValue = "true")
public class CloudWatchMetricsConfig {

    private static final Logger log = LoggerFactory.getLogger(CloudWatchMetricsConfig.class);

    /** The metrics an alarm can usefully watch. Everything else stays in Prometheus. */
    static final Set<String> EXPORTED = Set.of(
            // Trades the gateway knows it missed and could not recover: "no data loss" broken.
            "mdg.backfill.trades.missing",
            // Neither the broker nor the disk buffer could take an event; it went to the DLQ.
            "mdg.events.undeliverable",
            // The dead letter write itself failed: data loss.
            "mdg.dlq.publish.failed",
            // Events waiting on disk for the broker. Rising and staying up means the broker is down.
            "mdg.spill.pending",
            // The heartbeat. Zero for ten minutes means the feeds have stopped, whatever the reason.
            "mdg.events.published");

    @Bean(destroyMethod = "close")
    CloudWatchAsyncClient cloudWatchAsyncClient(CloudWatchProperties properties) {
        Region region = region(properties);
        // With no region the client cannot even be built, and an exception here would stop
        // the gateway from starting. Monitoring must never take the service down: build it
        // against a placeholder, and the registry below declines to export.
        return CloudWatchAsyncClient.builder().region(region != null ? region : Region.US_EAST_1).build();
    }

    @Bean(destroyMethod = "close")
    CloudWatchMeterRegistry cloudWatchMeterRegistry(CloudWatchProperties properties, CloudWatchAsyncClient client) {
        String problem = whyExportIsImpossible(properties);
        CloudWatchMeterRegistry registry = new CloudWatchMeterRegistry(
                config(properties.namespace(), properties.step(), problem == null), Clock.SYSTEM, client);
        // Filters must be in place before any meter registers, so they are added here, before
        // Spring hands the registry to the composite that the application writes to.
        exportOnly(registry);
        if (problem == null) {
            log.info("Exporting {} metrics to CloudWatch namespace '{}' every {}",
                    EXPORTED.size(), properties.namespace(), properties.step());
        } else {
            // Once, at startup, saying what to fix. Without this check the registry would try
            // every minute and log a full stack trace each time.
            log.error("CloudWatch export is enabled but cannot work, so no metrics will be sent: {}", problem);
        }
        return registry;
    }

    /** @return why metrics cannot be sent, or {@code null} if they can */
    static String whyExportIsImpossible(CloudWatchProperties properties) {
        if (region(properties) == null) {
            return "no AWS region found. Set AWS_REGION, or check the instance metadata hop limit is 2.";
        }
        try {
            DefaultCredentialsProvider.builder().build().resolveCredentials();
            return null;
        } catch (SdkClientException ex) {
            return "no AWS credentials found. Attach an IAM role with cloudwatch:PutMetricData to the "
                    + "instance (deploy/README.md), and check the instance metadata hop limit is 2. ("
                    + ex.getMessage() + ")";
        }
    }

    private static Region region(CloudWatchProperties properties) {
        if (!properties.region().isBlank()) {
            return Region.of(properties.region());
        }
        try {
            return DefaultAwsRegionProviderChain.builder().build().getRegion();
        } catch (SdkClientException ex) {
            return null;
        }
    }

    static void exportOnly(CloudWatchMeterRegistry registry) {
        registry.config()
                // One metric per name, not one per exchange: the alarm only needs the total, and
                // each extra dimension value would be another billed metric.
                .meterFilter(MeterFilter.ignoreTags("exchange"))
                .meterFilter(new MeterFilter() {
                    @Override
                    public MeterFilterReply accept(Meter.Id id) {
                        return EXPORTED.contains(id.getName()) ? MeterFilterReply.NEUTRAL : MeterFilterReply.DENY;
                    }
                });
    }

    static CloudWatchConfig config(String namespace, Duration step, boolean enabled) {
        return new CloudWatchConfig() {
            @Override
            public boolean enabled() {
                return enabled;   // false: meters still record, nothing is ever sent
            }

            @Override
            public String get(String key) {
                return null;   // everything not overridden takes Micrometer's default
            }

            @Override
            public String namespace() {
                return namespace;
            }

            @Override
            public Duration step() {
                return step;
            }
        };
    }
}
