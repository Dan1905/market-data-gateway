#!/usr/bin/env bash
# Deploys one release on the instance, and rolls back if it does not come up healthy.
#
#   deploy/deploy.sh <image> <git-ref>
#   deploy/deploy.sh ghcr.io/dan1905/market-data-gateway:v1.2.0 v1.2.0
#
# Run by the CD pipeline, over SSM or SSH, from the clone at /opt/market-data-gateway, as
# the user that owns it. It is the same script either way, which is why it lives in the
# repository rather than inline in the workflow.
#
# A release is the image AND its compose files (volumes, environment, memory limits live
# there), so both move together, and a rollback puts both back. Instance-specific settings
# belong in .env, which is never touched except for the GATEWAY_IMAGE line.
#
# Needs no registry credentials: the image is public on GHCR. That is deliberate. Over SSM
# every command is kept in the command history, so a token passed in would be stored there.
set -euo pipefail

IMAGE=${1:?usage: deploy.sh <image> <git-ref>}
REF=${2:?usage: deploy.sh <image> <git-ref>}
HEALTH_URL=${HEALTH_URL:-http://localhost:8080/actuator/health/liveness}
HEALTH_ATTEMPTS=${HEALTH_ATTEMPTS:-45}   # x 4 s = 3 minutes

cd "$(dirname "$0")/.."
echo "Deploying $IMAGE (compose files from $REF) in $(pwd)"

touch .env
PREVIOUS=$(grep '^GATEWAY_IMAGE=' .env | cut -d= -f2- || true)
echo "Previous image: ${PREVIOUS:-<none>}"

set_image() {
  grep -v '^GATEWAY_IMAGE=' .env > .env.next || true
  echo "GATEWAY_IMAGE=$1" >> .env.next
  grep -q '^COMPOSE_FILE=' .env.next \
    || echo 'COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml' >> .env.next
  mv .env.next .env
  chmod 600 .env
}

# Keep what is running now, so a failed release can be put back exactly.
cp docker-compose.yml .docker-compose.yml.prev
cp docker-compose.prod.yml .docker-compose.prod.yml.prev

git fetch --all --tags --force --quiet || true
if git rev-parse --verify --quiet "${REF}^{commit}" > /dev/null; then
  git checkout --quiet "$REF" -- docker-compose.yml docker-compose.prod.yml
  echo "Compose files updated to $REF"
else
  echo "WARNING: $REF is not a git ref here; keeping the existing compose files."
fi

restore_previous_files() {
  cp .docker-compose.yml.prev docker-compose.yml
  cp .docker-compose.prod.yml.prev docker-compose.prod.yml
  if [ -n "${PREVIOUS:-}" ]; then set_image "$PREVIOUS"; fi
}

set_image "$IMAGE"

# The image is public, but a saved login for ghcr.io (say, from an old deploy that ran
# `docker login` with a token that has since expired) makes the registry answer "denied"
# rather than fall back to anonymous access. So if the normal pull is refused, try once more
# with an empty Docker config: anonymous, and without touching any login saved on this host.
if ! docker compose pull market-data-gateway; then
  echo "Pull refused; retrying anonymously (a stale saved ghcr.io login can cause 'denied')"
  anonymous=$(mktemp -d)
  if ! DOCKER_CONFIG="$anonymous" docker compose pull market-data-gateway; then
    rm -rf "$anonymous"
    # Nothing has been restarted yet: put the files back so .env never names an image this
    # host cannot pull, and leave the running gateway exactly as it was.
    restore_previous_files
    echo "ERROR: could not pull $IMAGE. The running gateway was not touched."
    exit 1
  fi
  rm -rf "$anonymous"
fi
docker compose up -d --remove-orphans

echo "Waiting for the gateway to report healthy..."
for i in $(seq 1 "$HEALTH_ATTEMPTS"); do
  if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
    echo "Healthy after $((i * 4))s"
    docker image prune -f > /dev/null 2>&1 || true
    exit 0
  fi
  sleep 4
done

echo "ERROR: the gateway did not become healthy. Rolling back."
docker compose logs --tail=50 market-data-gateway || true

if [ -n "${PREVIOUS:-}" ]; then
  echo "Reverting to $PREVIOUS with the compose files it ran under"
  restore_previous_files
  docker compose up -d
else
  echo "No previous image recorded; leaving the stack as it is for inspection."
fi
exit 1
