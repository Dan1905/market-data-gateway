package com.mdg.gateway.config;

import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MockClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchMetricsConfigTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(CloudWatchMetricsConfig.class, PropertiesOnly.class);

    @EnableConfigurationProperties(CloudWatchProperties.class)
    static class PropertiesOnly {
    }

    @Test
    @DisplayName("off by default: no CloudWatch client or registry exists, so nothing ever calls AWS")
    void offByDefault() {
        context.run(ctx -> {
            assertThat(ctx).doesNotHaveBean(CloudWatchMeterRegistry.class);
            assertThat(ctx).doesNotHaveBean(CloudWatchAsyncClient.class);
        });
    }

    @Test
    @DisplayName("on when enabled, in the configured region and namespace")
    void onWhenEnabled() {
        context.withPropertyValues("gateway.cloudwatch.enabled=true", "gateway.cloudwatch.region=eu-north-1")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(CloudWatchMeterRegistry.class);
                    assertThat(ctx.getBean(CloudWatchAsyncClient.class).serviceClientConfiguration().region().id())
                            .isEqualTo("eu-north-1");
                    assertThat(ctx.getBean(CloudWatchProperties.class).namespace()).isEqualTo("MarketDataGateway");
                });
    }

    @Test
    @DisplayName("sends only the allowlisted metrics, one per name with exchanges summed, to the right namespace")
    void sendsOnlyTheAlertingMetrics() {
        CloudWatchAsyncClient client = mock(CloudWatchAsyncClient.class);
        when(client.putMetricData(any(PutMetricDataRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutMetricDataResponse.builder().build()));
        MockClock clock = new MockClock();
        CloudWatchMeterRegistry registry = new CloudWatchMeterRegistry(
                CloudWatchMetricsConfig.config("MarketDataGateway", Duration.ofMinutes(1), true), clock, client);
        CloudWatchMetricsConfig.exportOnly(registry);

        // Exported: missing trades on two exchanges, the heartbeat, and the spill gauge.
        registry.counter("mdg.backfill.trades.missing", "exchange", "BINANCE").increment(3);
        registry.counter("mdg.backfill.trades.missing", "exchange", "KRAKEN").increment(2);
        registry.counter("mdg.events.published").increment(100);
        AtomicInteger pending = registry.gauge("mdg.spill.pending", new AtomicInteger(7));
        // Not exported: everything else the gateway measures.
        registry.counter("mdg.frames.received").increment();
        registry.timer("mdg.events.publish.latency").record(Duration.ofMillis(5));
        registry.counter("jvm.gc.pause").increment();

        assertThat(registry.getMeters()).extracting(m -> m.getId().getName())
                .containsExactlyInAnyOrder("mdg.backfill.trades.missing", "mdg.events.published", "mdg.spill.pending");

        clock.add(Duration.ofMinutes(1));   // close the step, so its counts are reported
        registry.close();                   // publishes once more on the way out

        ArgumentCaptor<PutMetricDataRequest> sent = ArgumentCaptor.forClass(PutMetricDataRequest.class);
        verify(client, atLeastOnce()).putMetricData(sent.capture());
        List<MetricDatum> data = sent.getAllValues().stream().flatMap(r -> r.metricData().stream()).toList();

        assertThat(sent.getAllValues()).allSatisfy(r -> assertThat(r.namespace()).isEqualTo("MarketDataGateway"));
        // These exact names are what a CloudWatch alarm selects: Micrometer appends the statistic,
        // ".count" for a counter and ".value" for a gauge (deploy/README.md uses them).
        assertThat(data).extracting(MetricDatum::metricName).containsOnly(
                "mdg.backfill.trades.missing.count", "mdg.events.published.count", "mdg.spill.pending.value");
        // Both exchanges were folded into one metric with no exchange dimension: 3 + 2.
        MetricDatum missing = data.stream().filter(d -> d.metricName().equals("mdg.backfill.trades.missing.count"))
                .filter(d -> d.value() > 0).findFirst().orElseThrow();
        assertThat(missing.value()).isEqualTo(5.0);
        assertThat(missing.dimensions()).noneMatch(d -> d.name().equals("exchange"));
        assertThat(pending).isNotNull();
    }

    @Test
    @DisplayName("the allowlist fits CloudWatch's free tier of 10 custom metrics")
    void staysInsideTheFreeTier() {
        // Each exported name is one metric (the exchange tag is folded away), so this is the bill.
        assertThat(CloudWatchMetricsConfig.EXPORTED).hasSizeLessThanOrEqualTo(10);
        assertThat(Clock.SYSTEM).isNotNull();
    }

    @Test
    @DisplayName("without AWS credentials it starts, says once what to fix, and sends nothing")
    void noCredentialsMeansNoExportNotNoStartup() {
        // This test machine has no AWS credentials, which is exactly the state of an instance
        // whose IAM role was forgotten. Point the SDK away from any real config or metadata.
        System.setProperty("aws.accessKeyId", "");
        try {
            String problem = CloudWatchMetricsConfig.whyExportIsImpossible(
                    new CloudWatchProperties(true, "MarketDataGateway", Duration.ofMinutes(1), "eu-north-1"));
            if (problem != null) {
                assertThat(problem).contains("credentials");
            }
            CloudWatchAsyncClient client = mock(CloudWatchAsyncClient.class);
            CloudWatchMeterRegistry disabled = new CloudWatchMeterRegistry(
                    CloudWatchMetricsConfig.config("MarketDataGateway", Duration.ofMinutes(1), false), new MockClock(), client);
            disabled.counter("mdg.events.published").increment();
            disabled.close();
            org.mockito.Mockito.verifyNoInteractions(client);
        } finally {
            System.clearProperty("aws.accessKeyId");
        }
    }
}
