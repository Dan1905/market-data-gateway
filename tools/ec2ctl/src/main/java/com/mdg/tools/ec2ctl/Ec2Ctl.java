package com.mdg.tools.ec2ctl;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

/**
 * Start, stop and inspect the gateway's EC2 instance, and keep its DuckDNS name pointing at it.
 *
 * <pre>
 *   ec2ctl status        state, public IP, and whether DuckDNS points at it
 *   ec2ctl start         start, wait until running, update DuckDNS, wait until the gateway answers
 *   ec2ctl stop --yes    stop and wait until stopped
 *   ec2ctl sync-dns      point DuckDNS at the current IP if it is stale
 * </pre>
 *
 * <h2>Why it exists</h2>
 * Stopping the instance when it is not needed saves free-tier hours, but an automatically
 * assigned public IP is released on stop and a new one is given on start. That is what left
 * DuckDNS pointing at a dead address and broke a deploy. An Elastic IP avoids it too, but is
 * itself billed while the instance is stopped; this keeps the name right instead.
 *
 * <p>Configuration is environment variables only, so no account detail lands in the repository:
 * {@code MDG_INSTANCE_ID} (required), {@code DUCKDNS_DOMAIN} and {@code DUCKDNS_TOKEN} (for the
 * DNS steps), {@code MDG_HEALTH_URL} (defaults to the DuckDNS name's liveness endpoint). AWS
 * region and credentials come from the SDK's usual chain.
 */
public final class Ec2Ctl {

    static final URI DUCKDNS_API = URI.create("https://www.duckdns.org");

    private final Ec2Instances ec2;
    private final DuckDns dns;
    private final HealthCheck health;
    private final Map<String, String> env;
    private final PrintStream out;

    Ec2Ctl(Ec2Instances ec2, DuckDns dns, HealthCheck health, Map<String, String> env, PrintStream out) {
        this.ec2 = ec2;
        this.dns = dns;
        this.health = health;
        this.env = env;
        this.out = out;
    }

    public static void main(String[] args) {
        if (args.length == 0 || !java.util.List.of("status", "start", "stop", "sync-dns").contains(args[0])) {
            System.err.println("usage: ec2ctl status | start | stop --yes | sync-dns");
            System.exit(2);
        }
        String instanceId = System.getenv("MDG_INSTANCE_ID");
        if (instanceId == null || instanceId.isBlank()) {
            System.err.println("error: MDG_INSTANCE_ID is not set (e.g. export MDG_INSTANCE_ID=i-0abc123def4567890)");
            System.exit(1);
        }
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        try (Ec2Client client = Ec2Client.create()) {
            Ec2Ctl ctl = new Ec2Ctl(new Ec2Instances(client), new DuckDns(http, DUCKDNS_API),
                    new HealthCheck(http, Ec2Ctl::sleep), System.getenv(), System.out);
            System.exit(ctl.run(args));
        } catch (SdkClientException ex) {
            // The SDK's own messages list every provider it tried; say what to do instead.
            System.err.println("error: " + setupHint(ex.getMessage()));
            System.exit(1);
        } catch (RuntimeException ex) {
            System.err.println("error: " + ex.getMessage());
            System.exit(1);
        }
    }

    /** @return the process exit code */
    int run(String... args) {
        String instanceId = required("MDG_INSTANCE_ID");
        return switch (args[0]) {
            case "status" -> status(instanceId);
            case "start" -> start(instanceId);
            case "stop" -> stop(instanceId, args.length > 1 && "--yes".equals(args[1]));
            case "sync-dns" -> syncDns(ec2.describe(instanceId)) ? 0 : 1;
            default -> 2;
        };
    }

    private int status(String instanceId) {
        Instance instance = ec2.describe(instanceId);
        out.printf("instance   %s%nstate      %s%npublic IP  %s%n", instance.id(), instance.state(),
                instance.publicIp() == null ? "(none: stopped instances have no public IP)" : instance.publicIp());
        String domain = env.get("DUCKDNS_DOMAIN");
        if (domain != null && !domain.isBlank()) {
            String resolved = dns.resolve(domain);
            out.printf("DNS        %s.duckdns.org -> %s  (%s)%n", domain, resolved,
                    dnsVerdict(resolved, instance.publicIp()));
        }
        return 0;
    }

    private int start(String instanceId) {
        Instance before = ec2.describe(instanceId);
        if (before.isRunning()) {
            out.println("Already running at " + before.publicIp());
        } else {
            out.println("Starting " + instanceId + " (from " + before.state() + ")...");
        }
        Instance running = before.isRunning() ? before : ec2.startAndWait(instanceId);
        out.println("Running. Public IP " + running.publicIp());

        if (!syncDns(running)) {
            return 1;
        }
        URI url = healthUrl();
        if (url == null) {
            return 0;
        }
        out.println("Waiting for the gateway at " + url + " ...");
        // Up to ~6 minutes: Docker starts the stack, the JVM boots, and DNS caches may lag.
        if (health.waitUntilUp(url, 36, Duration.ofSeconds(10))) {
            out.println("Gateway is up.");
            return 0;
        }
        out.println("The instance is running but the gateway did not answer yet. Check with: ec2ctl status");
        return 1;
    }

    private int stop(String instanceId, boolean confirmed) {
        if (!confirmed) {
            // Stopping takes the public gateway offline and changes its IP on the next start,
            // so it must be asked for explicitly, never by a stray or half-typed command.
            out.println("This stops the instance and takes the gateway offline. Run: ec2ctl stop --yes");
            return 2;
        }
        Instance instance = ec2.describe(instanceId);
        if (instance.isStopped()) {
            out.println("Already stopped.");
            return 0;
        }
        out.println("Stopping " + instanceId + "...");
        ec2.stopAndWait(instanceId);
        out.println("Stopped. Its public IP is released; `ec2ctl start` will update DuckDNS to the new one.");
        return 0;
    }

    /** @return false only if an update was needed and failed */
    private boolean syncDns(Instance instance) {
        String domain = env.get("DUCKDNS_DOMAIN");
        String token = env.get("DUCKDNS_TOKEN");
        if (domain == null || domain.isBlank() || token == null || token.isBlank()) {
            out.println("DUCKDNS_DOMAIN / DUCKDNS_TOKEN not set: skipping the DNS update.");
            return true;
        }
        if (instance.publicIp() == null) {
            out.println("The instance has no public IP (state " + instance.state() + "): nothing to point DNS at.");
            return true;
        }
        String resolved = dns.resolve(domain);
        if (instance.publicIp().equals(resolved)) {
            out.println("DuckDNS already points at " + resolved + ".");
            return true;
        }
        try {
            dns.update(domain, token, instance.publicIp());
        } catch (IllegalStateException ex) {
            out.println(ex.getMessage());
            return false;
        }
        out.println("DuckDNS updated: " + domain + ".duckdns.org -> " + instance.publicIp()
                + (resolved == null ? "" : " (was " + resolved + ")"));
        return true;
    }

    static String setupHint(String sdkMessage) {
        String message = sdkMessage == null ? "" : sdkMessage;
        if (message.contains("Unable to load region")) {
            return "no AWS region. Set AWS_REGION (e.g. export AWS_REGION=eu-north-1) or run `aws configure`.";
        }
        if (message.contains("Unable to load credentials")) {
            return "no AWS credentials. Run `aws configure` (or `aws sso login`) for a user allowed to "
                    + "describe, start and stop this instance; see tools/ec2ctl/README.md.";
        }
        return message;
    }

    static String dnsVerdict(String resolved, String publicIp) {
        if (publicIp == null) {
            return "instance has no public IP";
        }
        if (resolved == null) {
            return "does not resolve";
        }
        return resolved.equals(publicIp) ? "up to date" : "STALE: run ec2ctl sync-dns";
    }

    private URI healthUrl() {
        String explicit = env.get("MDG_HEALTH_URL");
        if (explicit != null && !explicit.isBlank()) {
            return URI.create(explicit);
        }
        String domain = env.get("DUCKDNS_DOMAIN");
        return domain == null || domain.isBlank()
                ? null
                : URI.create("https://" + domain + ".duckdns.org/actuator/health/liveness");
    }

    private String required(String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is not set (e.g. export " + name + "=i-0abc123def4567890)");
        }
        return value;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
