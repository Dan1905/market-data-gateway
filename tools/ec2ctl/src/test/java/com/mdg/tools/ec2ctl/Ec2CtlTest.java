package com.mdg.tools.ec2ctl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class Ec2CtlTest {

    private static final String ID = "i-0abc123def4567890";

    private final Ec2Instances ec2 = mock(Ec2Instances.class);
    private final DuckDns dns = mock(DuckDns.class);
    private final HealthCheck health = mock(HealthCheck.class);
    private final Map<String, String> env = new HashMap<>();
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private Ec2Ctl ctl;

    @BeforeEach
    void setUp() {
        env.put("MDG_INSTANCE_ID", ID);
        env.put("DUCKDNS_DOMAIN", "example-gateway");
        env.put("DUCKDNS_TOKEN", "test-token");
        ctl = new Ec2Ctl(ec2, dns, health, env, new PrintStream(output));
    }

    private String out() {
        return output.toString();
    }

    @Test
    @DisplayName("status shows state and IP, and flags DNS pointing at an old address")
    void statusFlagsStaleDns() {
        when(ec2.describe(ID)).thenReturn(new Instance(ID, "running", "13.61.174.17"));
        when(dns.resolve("example-gateway")).thenReturn("13.60.88.157");

        assertThat(ctl.run("status")).isZero();

        assertThat(out()).contains("running", "13.61.174.17", "STALE");
        verify(dns, never()).update(any(), any(), any());
    }

    @Test
    @DisplayName("start: starts a stopped instance, points DNS at the new IP, waits for the gateway")
    void startFromStopped() {
        when(ec2.describe(ID)).thenReturn(new Instance(ID, "stopped", null));
        when(ec2.startAndWait(ID)).thenReturn(new Instance(ID, "running", "13.61.200.5"));
        when(dns.resolve("example-gateway")).thenReturn("13.61.174.17");
        when(health.waitUntilUp(any(), anyInt(), any())).thenReturn(true);

        assertThat(ctl.run("start")).isZero();

        verify(ec2).startAndWait(ID);
        verify(dns).update("example-gateway", "test-token", "13.61.200.5");
        verify(health).waitUntilUp(URI.create("https://example-gateway.duckdns.org/actuator/health/liveness"),
                36, java.time.Duration.ofSeconds(10));
        assertThat(out()).contains("DuckDNS updated", "13.61.200.5", "was 13.61.174.17", "Gateway is up");
    }

    @Test
    @DisplayName("start on a running instance with correct DNS changes nothing")
    void startWhenAlreadyRight() {
        when(ec2.describe(ID)).thenReturn(new Instance(ID, "running", "13.61.174.17"));
        when(dns.resolve("example-gateway")).thenReturn("13.61.174.17");
        when(health.waitUntilUp(any(), anyInt(), any())).thenReturn(true);

        assertThat(ctl.run("start")).isZero();

        verify(ec2, never()).startAndWait(any());
        verify(dns, never()).update(any(), any(), any());
        assertThat(out()).contains("Already running", "already points");
    }

    @Test
    @DisplayName("a DuckDNS refusal is reported and fails the command, without waiting on health")
    void dnsFailureFails() {
        when(ec2.describe(ID)).thenReturn(new Instance(ID, "running", "13.61.200.5"));
        when(dns.resolve("example-gateway")).thenReturn("13.61.174.17");
        doThrow(new IllegalStateException("DuckDNS refused the update (HTTP 200, body 'KO')"))
                .when(dns).update(any(), any(), any());

        assertThat(ctl.run("start")).isEqualTo(1);

        assertThat(out()).contains("KO");
        verifyNoInteractions(health);
    }

    @Test
    @DisplayName("stop refuses without --yes: taking the gateway offline must be explicit")
    void stopNeedsConfirmation() {
        assertThat(ctl.run("stop")).isEqualTo(2);

        verify(ec2, never()).stopAndWait(any());
        assertThat(out()).contains("ec2ctl stop --yes");
    }

    @Test
    @DisplayName("stop --yes stops a running instance")
    void stopConfirmed() {
        when(ec2.describe(ID)).thenReturn(new Instance(ID, "running", "13.61.174.17"));

        assertThat(ctl.run("stop", "--yes")).isZero();

        verify(ec2).stopAndWait(ID);
    }

    @Test
    @DisplayName("without DuckDNS settings the DNS step is skipped, not an error")
    void noDnsSettings() {
        env.remove("DUCKDNS_TOKEN");
        when(ec2.describe(ID)).thenReturn(new Instance(ID, "running", "13.61.174.17"));
        when(health.waitUntilUp(any(), anyInt(), any())).thenReturn(true);

        assertThat(ctl.run("sync-dns")).isZero();

        verify(dns, never()).update(any(), any(), any());
        assertThat(out()).contains("skipping the DNS update");
    }

    @Test
    @DisplayName("the instance id is required, with a message saying how to set it")
    void instanceIdRequired() {
        env.remove("MDG_INSTANCE_ID");

        assertThatThrownBy(() -> ctl.run("status"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("MDG_INSTANCE_ID");
    }

    @Test
    @DisplayName("DNS verdicts")
    void dnsVerdicts() {
        assertThat(Ec2Ctl.dnsVerdict("1.2.3.4", "1.2.3.4")).isEqualTo("up to date");
        assertThat(Ec2Ctl.dnsVerdict("1.2.3.4", "5.6.7.8")).startsWith("STALE");
        assertThat(Ec2Ctl.dnsVerdict(null, "5.6.7.8")).isEqualTo("does not resolve");
        assertThat(Ec2Ctl.dnsVerdict("1.2.3.4", null)).isEqualTo("instance has no public IP");
    }

    @Test
    @DisplayName("AWS setup errors are turned into one line saying what to do")
    void setupHints() {
        assertThat(Ec2Ctl.setupHint("Unable to load region from any of the providers in the chain ..."))
                .contains("AWS_REGION");
        assertThat(Ec2Ctl.setupHint("Unable to load credentials from any of the providers in the chain ..."))
                .contains("aws configure");
        assertThat(Ec2Ctl.setupHint("something else")).isEqualTo("something else");
    }
}
