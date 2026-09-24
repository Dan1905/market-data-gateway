# Deploying to EC2 — step by step

A linear path from nothing to a running gateway. Roughly 30 minutes.

The order is deliberate: **get it running privately first, expose it second.** Steps 1–8
put nothing on the public internet. Step 9 is where you decide how (or whether) to open it
up.

For the reasoning behind the sizing and security choices, see [README.md](README.md).

---

## Step 1 — Launch the instance

AWS Console → EC2 → **Launch instance**

| Setting | Value | Why |
|---|---|---|
| Name | `market-data-gateway` | |
| AMI | **Ubuntu Server 24.04 LTS** | Docker *and* the Compose v2 plugin are one `apt` command. On Amazon Linux you have to install the Compose plugin by hand. |
| Instance type | **t3.micro** | 2 vCPU, 1 GB. The stack is sized for exactly this. |
| Key pair | Create one, download the `.pem` | This is your only way in. |
| Storage | **12–16 GiB** gp3 | 8 GiB works but ends up ~75% full (~3 GB OS + ~1.3 GB images + ~1 GB topic data), so you will be pruning images. See the cost note below. |

**Network settings → Edit**, then allow only:

| Type | Port | Source |
|---|---|---|
| SSH | 22 | **My IP** |
| HTTP | 80 | Anywhere — *only if you plan to do step 9* |
| HTTPS | 443 | Anywhere — *only if you plan to do step 9* |

Do **not** open 8080, 8090 or 19092. Nothing needs them: every container binds to
`127.0.0.1` and only Caddy is ever public.

Launch it, then copy the **Public IPv4 DNS**.

### Will the extra disk cost anything?

Depends which free tier your account is on — AWS changed it.

| Your account | What you get | 16 GiB disk |
|---|---|---|
| Created **before** the change (legacy free tier) | 750 hrs/month t2/t3.micro + **30 GB EBS**, 12 months | **Free** |
| Created **after** (current free plan) | $100–200 in credits over 6 months | Draws credits |

Check under **Billing → Free tier**. If you see usage bars against 750 hrs and 30 GB, you
are on the legacy tier and anything up to 30 GB is included.

On the credit plan the disk is not the expensive part. Roughly, in us-east-1:

| Item | ~per month |
|---|---|
| t3.micro | ~$7.59 |
| 8 GiB gp3 | ~$0.64 |
| 16 GiB gp3 | ~$1.28 |

The extra 8 GiB is about **64 cents** against an instance costing twelve times that. Pick
8 GiB if you want to be strict and do not mind pruning images; 12–16 GiB otherwise.

---

## Step 2 — Connect

```bash
chmod 400 ~/Downloads/your-key.pem
ssh -i ~/Downloads/your-key.pem ubuntu@<public-dns>
```

Everything from here runs **on the instance** unless it says otherwise.

---

## Step 3 — Install Docker

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-v2 git
sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu
```

Log out and back in so the group takes effect:

```bash
exit
```
```bash
ssh -i ~/Downloads/your-key.pem ubuntu@<public-dns>
docker ps          # should print an empty table, not a permission error
```

---

## Step 4 — Add swap

**Do not skip this.** 1 GB with no swap is fragile; a single pull or restart spike can get
a container OOM-killed.

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/99-swap.conf
sudo sysctl -p /etc/sysctl.d/99-swap.conf
free -h            # Swap should show 2.0Gi
```

---

## Step 5 — Publish the image

**On your laptop**, not the instance.

The image is built by GitHub Actions and pulled by the instance. It is never built on the
instance — a Maven build needs far more memory than 1 GB, and it would be OOM-killed even
with swap.

```bash
git push origin main
```

Watch **Actions → CD**. The `publish` job prints the image name in its summary, e.g.
`ghcr.io/dan1905/market-data-gateway:sha-4a16692`. Copy that.

Then make the package readable by the instance — **Your profile → Packages →
market-data-gateway → Package settings → Change visibility → Public**.

> Prefer to keep it private? Leave it, and on the instance run
> `docker login ghcr.io -u <you>` with a personal access token that has `read:packages`.

---

## Step 6 — Get the code onto the instance

```bash
sudo mkdir -p /opt/market-data-gateway
sudo chown ubuntu:ubuntu /opt/market-data-gateway
git clone https://github.com/Dan1905/market-data-gateway.git /opt/market-data-gateway
cd /opt/market-data-gateway
```

---

## Step 7 — Configure

```bash
cat > .env <<'EOF'
COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml
GATEWAY_IMAGE=ghcr.io/dan1905/market-data-gateway:sha-XXXXXXX
EOF
chmod 600 .env
```

Replace `sha-XXXXXXX` with the tag from step 5.

`COMPOSE_FILE` is what makes the instance **pull** the published image instead of trying
to build one. Compose reads it from `.env` automatically, so every command below is a
plain `docker compose ...` with no flags to remember.

---

## Step 8 — Start it

```bash
docker compose up -d
```

Caddy is in the `public` profile, which is not active yet — so nothing is exposed. Step 9
turns it on.

Wait about 30 seconds, then check:

```bash
docker compose ps
curl -s localhost:8080/actuator/health/exchanges
```

All three venues should report `"connected": true`. If so, you are done — the gateway is
ingesting live market data and publishing normalized events to Kafka.

Watch it work:

```bash
docker exec redpanda rpk topic consume normalized-market-data --num 5
docker stats --no-stream
```

Memory should total roughly 600–700 MB once warm.

### Look at it from your laptop, without exposing anything

```bash
ssh -i ~/Downloads/your-key.pem -L 8080:localhost:8080 -L 8090:localhost:8090 ubuntu@<public-dns>
```

Leave that running and open:

- <http://localhost:8090> — Redpanda Console, live topics
- <http://localhost:8080/asyncapi.html> — the event contract
- <http://localhost:8080/swagger-ui.html> — DLQ replay

**If you only need to demo it yourself, stop here.** Steps 9–11 are for making it publicly
reachable and self-managing.

---

## Step 9 — Make it public (optional)

Pick one.

### Option A — You have a domain

Point an `A` record at the instance's public IP and wait for it to resolve. Then:

```bash
# generate a password hash — copy the whole $2a$... string
docker run --rm caddy:2-alpine caddy hash-password --plaintext 'a-strong-password'
```

```bash
cat >> .env <<'EOF'
COMPOSE_PROFILES=public
SITE_ADDRESS=gateway.example.com
GATEWAY_BASIC_AUTH_USER=admin
GATEWAY_BASIC_AUTH_HASH=$2a$14$paste-the-hash-here
EOF

docker compose up -d
```

`COMPOSE_PROFILES=public` is what starts Caddy. Remove that line and re-run to go private
again.

Caddy obtains a Let's Encrypt certificate automatically. Verify:

```bash
curl -s https://gateway.example.com/actuator/health/liveness          # public, 200
curl -su admin:'a-strong-password' https://gateway.example.com/actuator/health/exchanges
```

### Option B — No domain, want a free one with real HTTPS

Register a free subdomain at <https://duckdns.org> pointing at your instance IP, then
follow Option A using `yourname.duckdns.org`. You get genuine TLS.

### Option C — No domain, HTTP only

Set `SITE_ADDRESS=:80` in `.env` and follow Option A otherwise.

⚠️ Your basic-auth password crosses the network **in plaintext**. Acceptable for a
throwaway demo; do not reuse a password you care about. Option B is barely more work and
is properly encrypted.

---

## Step 10 — Start on boot (optional)

```bash
sudo cp deploy/market-data-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now market-data-gateway
systemctl status market-data-gateway
```

No edits needed whether or not you did step 9 — the unit reads `.env`, so the profile and
file list come from there.

---

## Step 11 — Deploy future changes (optional)

Add three repository secrets — **Settings → Secrets and variables → Actions**:

| Secret | Value |
|---|---|
| `EC2_HOST` | the public DNS |
| `EC2_USER` | `ubuntu` |
| `EC2_SSH_KEY` | contents of your `.pem`, including the BEGIN/END lines |

Use a dedicated key rather than your login key:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/mdg-deploy -C "gh-actions" -N ""
ssh-copy-id -i ~/.ssh/mdg-deploy.pub ubuntu@<public-dns>
```

Then releasing is:

```bash
git tag v1.0.0
git push origin v1.0.0
```

CD verifies, publishes, deploys, health-checks, and rolls back automatically if the new
container does not come up.

---

## Day-to-day

```bash
cd /opt/market-data-gateway

docker compose ps                                   # what is running
docker compose logs -f market-data-gateway          # logs
docker stats --no-stream                            # memory
curl -s localhost:8080/actuator/health/exchanges    # venue connections
df -h /                                             # disk

docker exec redpanda rpk topic consume market-data-dlq --print-headers -o :end -n 10

# update to a new image by hand
sed -i 's|^GATEWAY_IMAGE=.*|GATEWAY_IMAGE=ghcr.io/dan1905/market-data-gateway:sha-NEW|' .env
docker compose up -d

docker compose down        # stop, keep data
docker compose down -v     # stop and wipe topics
```

---

## When something is wrong

**A venue shows `connected: false`** — usually Coinbase, which has been moving channels
behind JWT auth. Turn it off and restart:

```bash
# set COINBASE_ENABLED=false under market-data-gateway in docker-compose.yml
docker compose up -d market-data-gateway
```

**Gateway keeps restarting** — `docker compose logs market-data-gateway`. If it is OOM,
confirm swap is on with `free -h`.

**`docker compose pull` says denied** — the GHCR package is still private. Make it public,
or `docker login ghcr.io` with a `read:packages` token.

**Disk filling up** — `docker system prune -a` reclaims old images. Topic retention is 24h
by default (~1 GB); see README.md §6 before raising it.

**Caddy will not start** — `docker compose logs caddy`. Almost always DNS not yet pointing
at the instance, or port 80/443 missing from the security group.
