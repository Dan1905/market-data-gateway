package com.mdg.gateway.web;

import com.mdg.gateway.client.ExchangeConnectionManager;
import com.mdg.gateway.model.Exchange;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Reports venue connectivity on {@code /actuator/health/exchanges}.
 *
 * <h2>Why this is deliberately not wired into readiness</h2>
 * A venue outage is not a reason to pull this instance out of the load balancer or to have
 * an orchestrator restart it — restarting does not bring Binance back, and the reconnect
 * loop is already the correct response. Worse, if all instances report unhealthy during a
 * venue incident, an autoscaler can cycle the entire fleet and lose the DLQ replay endpoint
 * exactly when it is needed.
 *
 * <p>So this contributes {@code DOWN} to the detailed health view — where it should raise an
 * alert — while the pipeline itself stays serving. Registering a venue in liveness or
 * readiness is a decision to make per deployment, not a default.
 */
@Component("exchanges")
public class ExchangeHealthIndicator implements HealthIndicator {

    private final ExchangeConnectionManager connectionManager;

    public ExchangeHealthIndicator(ExchangeConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public Health health() {
        Map<Exchange, Boolean> status = connectionManager.connectionStatus();
        Map<Exchange, Integer> failures = connectionManager.consecutiveFailures();

        if (status.isEmpty()) {
            return Health.up()
                    .withDetail("feeds", "none enabled")
                    .build();
        }

        boolean allConnected = status.values().stream().allMatch(Boolean::booleanValue);
        Health.Builder builder = allConnected ? Health.up() : Health.down();

        status.forEach((exchange, connected) -> builder.withDetail(
                exchange.name(),
                Map.of("connected", connected,
                        "consecutiveFailures", failures.getOrDefault(exchange, 0))));

        return builder.build();
    }
}
