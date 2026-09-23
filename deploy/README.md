# Deploying to AWS EC2 free tier

Target: **t3.micro — 2 vCPU, 1 GB RAM**, Amazon Linux 2023 or Ubuntu 24.04.
Free tier covers 750 instance-hours/month for 12 months.

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

`normalized-market-data` retention therefore defaults to **24h**, not 7 days — 7 days
would need ~7.6 GB and an EC2 root volume defaults to 8 GB, leaving no room for the OS,
Docker images and logs.

```yaml
gateway:
  topics:
    normalized-retention: 24h    # ~1 GB
    dead-letter-retention: 7d    # tiny; dead letters should be rare
```

Raise it only after attaching a bigger volume — the free tier includes 30 GB of EBS, so
a 20 GB volume supports about two weeks.

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

### Instance prerequisites

The pipeline assumes a clone at `/opt/market-data-gateway` and a `.env` that already
contains `SITE_ADDRESS` and `GATEWAY_BASIC_AUTH_HASH` (sections 2 and 3 above). The
pipeline only ever rewrites the `GATEWAY_IMAGE` line — your credentials survive deploys.

Make the package readable by the instance: GHCR packages default to private, so either
make the package public (Packages → market-data-gateway → Settings → Change visibility)
or `docker login ghcr.io` on the instance with a PAT that has `read:packages`.

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

The pipeline also rolls back on its own: it records the running image before switching,
and if the new container does not report healthy within ~3 minutes it restores the
previous one and fails the run.

### Deploying by hand

```bash
ssh ec2-user@<host>
cd /opt/market-data-gateway
echo "GATEWAY_IMAGE=ghcr.io/<owner>/market-data-gateway:v1.0.0" >> .env
docker compose up -d      # COMPOSE_FILE and COMPOSE_PROFILES come from .env
```
