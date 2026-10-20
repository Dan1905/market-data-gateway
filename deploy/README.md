# Deploying to AWS EC2 free tier

Target: **t3.micro — 2 vCPU, 1 GB RAM**, Ubuntu 24.04 (or Amazon Linux 2023).

**On cost:** AWS replaced the free tier. Accounts created before the change keep the
legacy allowance — 750 instance-hours/month and 30 GB of EBS for 12 months — and this
stack fits inside it. Newer accounts get $100–200 in credits over 6 months instead, where
a t3.micro runs about $7.59/month and the disk a further $0.64–1.28. Check which you have
under **Billing → Free tier**.

---

## What you get

| URL | What it is | Auth |
|---|---|---|
| `/asyncapi.html` | **The event contract.** Kafka topics, `CanonicalTradeEvent` schema, DLQ header envelope | basic |
| `/swagger-ui.html` | HTTP control plane — DLQ replay, try-it-out | basic |
| `/console` | Redpanda Console — browse live topics and messages | basic |
| `/actuator/health` | Full health, including per-venue connection state | basic |
| `/actuator/prometheus` | Metrics scrape endpoint | basic |
| `/actuator/health/liveness` | Bare `{"status":"UP"}` for uptime monitors | **public** |

Only Caddy binds a public port. The gateway, broker and console are published on
`127.0.0.1` and are unreachable except through it.

---

## 1. Instance setup

Security group: inbound **22** (your IP only), **80** and **443** (anywhere). Nothing else —
in particular not 19092 (Kafka), 8080 or 8090.

```bash
# Amazon Linux 2023
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user   # log out and back in

# Ubuntu 24.04
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu
```

### Add swap — do not skip this

A 1 GB instance has no swap by default. The stack fits in RAM at steady state (~634 MB),
but `docker compose build` runs a Maven build that will exhaust memory and get OOM-killed
without it.

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
sudo sysctl vm.swappiness=10
echo 'vm.swappiness=10' | sudo tee -a /etc/sysctl.d/99-swap.conf
```

> **Better still: don't build on the instance.** Build the image on a machine with real
> RAM, push to a registry, and have the instance pull it. Replace the `build:` block in
> `docker-compose.yml` with `image: ghcr.io/your-org/market-data-gateway:1.0.0`.

---

## 2. Configure

```bash
sudo mkdir -p /opt/market-data-gateway
sudo chown "$USER" /opt/market-data-gateway
git clone <your-repo> /opt/market-data-gateway
cd /opt/market-data-gateway
```

Generate a password hash and write `.env` (gitignored — never commit it):

```bash
docker run --rm caddy:2-alpine caddy hash-password --plaintext 'pick-a-real-password'
```

```bash
cat > .env <<'EOF'
SITE_ADDRESS=gateway.example.com
GATEWAY_BASIC_AUTH_USER=admin
GATEWAY_BASIC_AUTH_HASH=$2a$14$paste.the.hash.from.above
EOF
chmod 600 .env
```

Point an A record at the instance's public IP before starting — Caddy needs the hostname
to resolve in order to complete the Let's Encrypt challenge. For a private instance with
no domain, leave `SITE_ADDRESS=:80` and reach it over an SSH tunnel instead:

```bash
ssh -L 8080:localhost:8080 -L 8090:localhost:8090 ec2-user@<ip>
```

---

## 3. Run

See [EC2-QUICKSTART.md](EC2-QUICKSTART.md) for the click-by-click version of this.

```bash
docker compose up -d --build
```

> **Confirm the build actually succeeded.** If the Maven build inside the image fails,
> compose aborts and starts nothing — which is correct. The trap is then running
> `docker compose up -d` *without* `--build`: that silently starts the previous, stale
> image, and everything reports healthy while running old code. If a build fails, fix it
> and re-run with `--build`. Check with `docker images market-data-gateway --format '{{.CreatedSince}}'`.

```bash
sudo cp deploy/market-data-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now market-data-gateway
```

Verify:

```bash
curl -s https://gateway.example.com/actuator/health/liveness       # public
curl -su admin:'your-password' https://gateway.example.com/actuator/health/exchanges
docker stats --no-stream
```

All three venues should report `"connected": true` within about 15 seconds.

---

## 4. Memory budget

Limits are ceilings, not reservations. The middle column is measured with all three
venues connected and ticks flowing.

| Service | Limit | Observed | Notes |
|---|---|---|---|
| redpanda | 320 MB | 236 MB | `--memory 200M --overprovisioned --smp 1` |
| market-data-gateway | 400 MB | 307 MB | 271 MB max heap, ~60 MB live |
| console | 128 MB | 76 MB | stateless |
| caddy | 32 MB | 15 MB | |
| **Total** | **880 MB** | **~634 MB** | ~250 MB free on a 1 GB box |

Measured after 16h of continuous ingest, not at startup — Redpanda grows into its
allocator. It reported 110 MB after a minute and 292 MB after 16h on the earlier
`--memory 256M`, which was **91% of its limit** and one spike from an OOM kill on the
broker. `--memory 200M` is what makes that safe.

The limits sum to under 1 GB deliberately: a simultaneous spike across every service still
cannot invoke the kernel OOM killer, which on the broker would lose data the gateway has
already acknowledged.

---

## 5. Metrics without a bigger instance

`/actuator/prometheus` exposes 12 custom metrics:

| Metric | Alert on |
|---|---|
| `mdg_frames_received_total` | Flatlines → a venue went silent |
| `mdg_frames_transform_failed_total` | Rising → schema drift at a venue |
| `mdg_dlq_published_total` | Rising → normalization is failing |
| `mdg_dlq_publish_failed_total` | **Any value > 0 is data loss** |
| `mdg_frames_throttled_total` | Ingest rate limiter shedding load |
| `mdg_events_publish_fallback_total` | Retries exhausted or breaker open |
| `mdg_events_publish_latency_seconds` | p99 climbing → broker degrading |
| `mdg_ws_disconnected_total` | Venue connection churn |

Self-hosting Prometheus (~150 MB) and Grafana (~120 MB) pushes the limit total to ~1.1 GB
and does not fit. Two options that do:

**Grafana Cloud free tier** (10k series, 14-day retention). Run the Alloy agent — about
50 MB — and ship metrics off the box:

```yaml
  alloy:
    image: grafana/alloy:latest
    container_name: alloy
    command: ["run", "/etc/alloy/config.alloy"]
    volumes:
      - ./deploy/config.alloy:/etc/alloy/config.alloy:ro
    environment:
      GRAFANA_CLOUD_URL: ${GRAFANA_CLOUD_URL}
      GRAFANA_CLOUD_USER: ${GRAFANA_CLOUD_USER}
      GRAFANA_CLOUD_TOKEN: ${GRAFANA_CLOUD_TOKEN}
    mem_limit: 64m
    restart: unless-stopped
```

scraping `market-data-gateway:8080/actuator/prometheus` every 30s and remote-writing to
your Grafana Cloud endpoint.

**Or move to t3.small** (2 GB, ~$15/month) and self-host the full stack.

---

## 5b. Alerts by email: CloudWatch

The gateway can push the five metrics worth being woken up for to AWS CloudWatch, where an
alarm emails you. It uses the AWS SDK with the instance's IAM role, so no AWS keys are ever
configured. Off unless you turn it on.

| Metric in CloudWatch | Means |
|---|---|
| `mdg.backfill.trades.missing.count` | Trades the gateway knows it missed and could not recover |
| `mdg.events.undeliverable.count` | Neither the broker nor the disk buffer could take an event |
| `mdg.dlq.publish.failed.count` | The dead letter write itself failed |
| `mdg.spill.pending.value` | Events waiting on disk because the broker is down |
| `mdg.events.published.count` | The heartbeat: trades reaching the topic |

Micrometer adds `.count` (counters) and `.value` (gauges) to the names; alarms use these.

**Cost.** 5 custom metrics, 3 alarms, one API call a minute and a few emails sit inside
CloudWatch's always-free tier (10 metrics, 10 alarms, 1M API requests) and SNS's (1,000
emails). Only the five are sent; everything else stays in Prometheus.

**1. Let the instance write metrics.** On the `mdg-instance` role (see section 8), add an
inline policy. `PutMetricData` cannot be limited to a resource, so it is limited to this
namespace instead:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "cloudwatch:PutMetricData",
    "Resource": "*",
    "Condition": { "StringEquals": { "cloudwatch:namespace": "MarketDataGateway" } }
  }]
}
```

**2. Let containers reach the role.** EC2 → the instance → Actions → Instance settings →
Modify instance metadata options → *Metadata response hop limit* = **2**. A container is one
network hop further from the metadata service than the host; with a limit of 1 the SDK finds
no credentials.

**3. Turn it on.** In `/opt/market-data-gateway/.env`:

```
CLOUDWATCH_ENABLED=true
AWS_REGION=eu-north-1
```

then `docker compose up -d`. The log says `Exporting 5 metrics to CloudWatch`. If the role or
the hop limit is wrong it says so once, in one line, and the gateway carries on without it.

**4. Where alerts go.** SNS → Topics → Create topic (Standard), `mdg-alerts` → Create
subscription → protocol *Email*, your address → confirm from the email AWS sends.

**5. The alarms.** CloudWatch → Alarms → Create alarm → Select metric → *MarketDataGateway*:

| Alarm | Metric | Statistic, period | Condition | Missing data |
|---|---|---|---|---|
| Trades lost | `mdg.backfill.trades.missing.count` | Sum, 5 min | > 0 for 1 of 1 | not breaching |
| Events dead-lettered | `mdg.events.undeliverable.count` | Sum, 5 min | > 0 for 1 of 1 | not breaching |
| Feed stopped | `mdg.events.published.count` | Sum, 5 min | ≤ 0 for 2 of 2 | **breaching** |

Send each to `mdg-alerts`. "Feed stopped" treats missing data as breaching on purpose: if the
gateway or the whole instance goes down, no data arrives at all, and that must alert too.

## 6. Operating it

```bash
# Logs
docker compose logs -f market-data-gateway
journalctl -u market-data-gateway -f

# What is actually on the topics
docker exec redpanda rpk topic consume normalized-market-data --num 5
docker exec redpanda rpk topic consume market-data-dlq --num 5 --print-headers

# Dead letters accumulating? Size a replay first.
curl -su admin:'pw' -X POST https://gateway.example.com/api/v1/dlq/replay \
     -H 'Content-Type: application/json' -d '{"dryRun": true}'

# Turn a venue off without redeploying
docker compose stop market-data-gateway
# edit COINBASE_ENABLED=false in docker-compose.yml
docker compose up -d market-data-gateway
```

### Disk

Measured on a live 16h run: **25 events/sec, ~1 GB/day on disk.**

Budget another **256 MB** for the gateway's spill buffer (on the `gateway-state` volume, which
also holds the few bytes of saved trade positions that let a redeploy recover what it missed). It is
empty in normal running and only fills while the broker is unavailable; `SPILL_MAX_BYTES`
in `.env` changes the cap.

`normalized-market-data` retention therefore defaults to **24h**, not 7 days — 7 days
would need ~7.6 GB and an EC2 root volume defaults to 8 GB, leaving no room for the OS,
Docker images and logs.

```yaml
gateway:
  topics:
    normalized-retention: 24h    # ~1 GB
    dead-letter-retention: 7d    # tiny; dead letters should be rare
```

Raise it only after attaching a bigger volume. The legacy free tier includes 30 GB of
EBS, so a 20 GB volume supports about two weeks of retention at no cost; on the credit
plan that volume is roughly $1.60/month.

**Retention applies at topic creation.** Changing it later needs an explicit alter:

```bash
docker exec redpanda rpk topic alter-config normalized-market-data --set retention.ms=86400000
```

---

## 7. Security checklist

- [ ] Security group exposes only 22 (your IP), 80, 443
- [ ] `.env` is `chmod 600` and gitignored; the bcrypt hash is not in version control
- [ ] Everything except `/actuator/health/liveness` sits behind basic auth
- [ ] Kafka (19092) and the admin API (9644) bind `127.0.0.1` only
- [ ] TLS is live — check `curl -I https://your-host` returns HSTS
- [ ] Swap is configured
- [ ] You have tested a `dryRun` replay before ever running a real one

The gateway has **no application-level authentication**. Caddy is the entire security
boundary. If you later expose the service another way — an ALB, a service mesh, a second
ingress — that path needs its own authentication, or the replay endpoint is open again.

---

## 8. Continuous deployment

`.github/workflows/cd.yml` builds the image on a runner, publishes it to GHCR, and the
instance pulls it. **The image is never built on the instance** — `docker compose build`
runs a full Maven build, which a 1 GB t3.micro cannot survive even with swap.

| Trigger | What happens |
|---|---|
| push to `main` | verify → publish `ghcr.io/<owner>/<repo>:sha-abc1234`. No deploy. |
| push tag `v*` | verify → publish `:v1.0.0` → **deploy that tag** |
| manual dispatch | deploy an already-published tag you name |

Deploying on a tag rather than on every green `main` makes releases explicit, and makes
rollback a matter of re-running the workflow with an older tag.

### Repository secrets

Settings → Secrets and variables → Actions:

| Secret | Value |
|---|---|
| `EC2_HOST` | public DNS or IP of the instance |
| `EC2_USER` | `ec2-user` (Amazon Linux) or `ubuntu` |
| `EC2_SSH_KEY` | the **private** key, whole file including the BEGIN/END lines |

`GITHUB_TOKEN` is provided automatically and is what authenticates the GHCR push.

Generate a deploy-only key rather than reusing your personal one:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/mdg-deploy -C "github-actions-deploy" -N ""
ssh-copy-id -i ~/.ssh/mdg-deploy.pub ec2-user@<host>
```

Paste the contents of `~/.ssh/mdg-deploy` (the private half) into `EC2_SSH_KEY`.

### Deploying without SSH: Systems Manager + OIDC (recommended)

By default the deploy step logs in over SSH, which means GitHub's runners must be able to
reach port 22. They connect from a large, changing set of addresses, so in practice port 22
ends up open to the whole internet. Systems Manager (SSM) removes that: the pipeline asks
AWS to run `deploy/deploy.sh`, and the SSM agent that already runs on Ubuntu's EC2 images
executes it. The instance needs **no inbound port** for deploys, and GitHub holds **no AWS
keys**: it trades a short-lived OIDC token for credentials on each run. Both are free.

The pipeline switches to SSM on its own as soon as `AWS_DEPLOY_ROLE_ARN` is set, and uses
SSH until then. One-time setup in the AWS console, in the instance's region:

**1. Let the instance talk to Systems Manager.**
IAM → Roles → Create role → *AWS service*, *EC2* → attach `AmazonSSMManagedInstanceCore` →
name it `mdg-instance`. Then EC2 → your instance → Actions → Security → Modify IAM role →
`mdg-instance`. Within a few minutes the instance appears as *Online* under Systems Manager →
Fleet Manager. If it does not: `sudo snap restart amazon-ssm-agent` on the instance.

**2. Let GitHub sign in to AWS.**
IAM → Identity providers → Add provider → *OpenID Connect*:
provider URL `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`.

**3. Create the role the deploy job assumes.**
IAM → Roles → Create role → *Custom trust policy*, and paste (replace `<ACCOUNT_ID>`):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
        "token.actions.githubusercontent.com:sub": "repo:Dan1905/market-data-gateway:environment:production"
      }
    }
  }]
}
```

The `sub` condition means only this repository's `production` deploy job can use the role:
not other repos, not pull requests, not other jobs. Skip the permissions page, name it
`mdg-github-deploy`, then add this inline policy to it (Permissions → Add permissions →
Create inline policy → JSON), replacing the three placeholders:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ssm:SendCommand",
      "Resource": [
        "arn:aws:ec2:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID>",
        "arn:aws:ssm:<REGION>::document/AWS-RunShellScript"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "ssm:GetCommandInvocation",
      "Resource": "*"
    }
  ]
}
```

It can run a command on this one instance and read the result, nothing else.

**4. Tell the workflow.** Settings → Secrets and variables → Actions:

| Kind | Name | Value |
|---|---|---|
| Secret | `AWS_DEPLOY_ROLE_ARN` | the role's ARN, `arn:aws:iam::<ACCOUNT_ID>:role/mdg-github-deploy` |
| Variable | `AWS_REGION` | the instance's region, e.g. `eu-north-1` |
| Variable | `EC2_INSTANCE_ID` | e.g. `i-0abc123def4567890` |

`EC2_HOST` and `EC2_USER` stay: the first for the public health check after a deploy, the
second because the script runs as the user that owns the clone. `EC2_SSH_KEY` is no longer
used once SSM is set up.

**5. Try it without releasing anything.** Actions → CD → Run workflow → enter the tag that is
already live. The run summary says *Deploying over SSM*, and the instance's output is printed
in the log.

**6. Close port 22.** Set the security group's SSH rule back to *My IP*, or delete it: for a
shell on the instance, EC2 → Connect → *Session Manager* now works without SSH or keys.

### Instance prerequisites

The pipeline assumes a clone at `/opt/market-data-gateway` and a `.env` that already
contains `SITE_ADDRESS` and `GATEWAY_BASIC_AUTH_HASH` (sections 2 and 3 above). The
pipeline only ever rewrites the `GATEWAY_IMAGE` line — your credentials survive deploys.

Each deploy also checks out `docker-compose.yml` and `docker-compose.prod.yml` for the tag
being deployed, because a release is the image *and* its compose file: volumes, environment
and memory limits live there. Keep instance-specific settings in `.env`, not in those two
files; local edits to them are overwritten (the previous copies are kept as
`.docker-compose*.prev` and restored on rollback).

Make the package readable by the instance: GHCR packages default to private, so make it
public (Packages → market-data-gateway → Settings → Change visibility). The deploy script
deliberately passes no registry credentials: over SSM every command is kept in the command
history, and a token passed in would be stored there.

If a pull is ever refused with `denied` although the image is public, the cause is a saved
`docker login ghcr.io` on the instance whose token has expired (older versions of this
pipeline logged in on every deploy). `deploy.sh` retries anonymously in that case; to clear
it for good, run `docker logout ghcr.io` on the instance.

### Releasing

```bash
git tag v1.0.0
git push origin v1.0.0
```

The deploy job targets a `production` environment. Create it under Settings →
Environments to require a manual approval before anything touches the instance.

### Rollback

Re-run the workflow against the previous tag — Actions → CD → Run workflow → enter
`v0.9.0`. That skips the build entirely and just repoints the instance.

The pipeline also rolls back on its own (`deploy/deploy.sh`): it records the running image
and compose files before switching, and if the new container does not report healthy
within ~3 minutes it restores both and fails the run.

### Deploying by hand

```bash
ssh ec2-user@<host>
cd /opt/market-data-gateway
echo "GATEWAY_IMAGE=ghcr.io/<owner>/market-data-gateway:v1.0.0" >> .env
docker compose up -d      # COMPOSE_FILE and COMPOSE_PROFILES come from .env
```
