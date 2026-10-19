package com.mdg.gateway.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Export of the alerting metrics to AWS CloudWatch (see {@code CloudWatchMetricsConfig}).
 *
 * @param enabled   off by default: only the deployed instance, which has an IAM role allowed to
 *                  write metrics, turns it on. Local runs and tests never call AWS.
 * @param namespace the CloudWatch namespace the metrics appear under
 * @param step      how often metrics are sent. One minute is CloudWatch's standard resolution;
 *                  finer would cost more and alerting on lost trades does not need it.
 * @param region    AWS region, e.g. {@code eu-north-1}. Blank means the SDK's default lookup
 *                  (the {@code AWS_REGION} variable, then the instance metadata).
 */
@Validated
@ConfigurationProperties(prefix = "gateway.cloudwatch")
public record CloudWatchProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("MarketDataGateway") @NotBlank String namespace,
        @DefaultValue("1m") @NotNull Duration step,
        @DefaultValue("") String region) {
}
