#!/usr/bin/env python3
"""Start, stop and inspect the gateway's EC2 instance, and keep its DuckDNS name pointing at it.

    ec2ctl.py status        state, public IP, and whether DuckDNS points at it
    ec2ctl.py start         start, wait until running, update DuckDNS, wait until the gateway answers
    ec2ctl.py stop --yes    stop and wait until stopped
    ec2ctl.py sync-dns      point DuckDNS at the current IP if it is stale

Why it exists: stopping the instance when it is not needed saves free-tier hours, but AWS
releases an automatically assigned public IP on stop and hands out a new one on start. That
left DuckDNS pointing at a dead address and broke a deploy. An Elastic IP fixes the address
but is billed while the instance is stopped; this keeps the name right instead.

Configuration is environment variables only, so no account detail lands in the repository:
MDG_INSTANCE_ID (required), DUCKDNS_DOMAIN and DUCKDNS_TOKEN (for the DNS steps), and
MDG_HEALTH_URL (defaults to the DuckDNS name's liveness endpoint). AWS region and credentials
come from boto3's usual lookup: environment, ~/.aws (what `aws configure` writes), SSO.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Mapping, TextIO

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, NoRegionError, PartialCredentialsError

DUCKDNS_API = "https://www.duckdns.org"

# `start` waits up to this long for the gateway after the instance is running: Docker brings
# the stack up, the JVM boots, and DNS caches may still hold the old address for a while.
HEALTH_ATTEMPTS = 36
HEALTH_INTERVAL_SECONDS = 10


@dataclass(frozen=True)
class Instance:
    id: str
    state: str
    # None while stopped: a stopped instance has no automatically assigned public IP.
    public_ip: str | None

    @property
    def running(self) -> bool:
        return self.state == "running"

    @property
    def stopped(self) -> bool:
        return self.state == "stopped"


class DnsError(Exception):
    """DuckDNS refused the update or could not be reached."""


# ---------------------------------------------------------------------------
# EC2, through boto3
# ---------------------------------------------------------------------------

def describe(ec2, instance_id: str) -> Instance:
    reservations = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"]
    instances = [i for r in reservations for i in r["Instances"]]
    if not instances:
        raise LookupError(f"No instance {instance_id} in this account and region")
    i = instances[0]
    return Instance(i["InstanceId"], i["State"]["Name"], i.get("PublicIpAddress"))


def start_and_wait(ec2, instance_id: str) -> Instance:
    """Start the instance and block until EC2 reports it running (typically under a minute)."""
    ec2.start_instances(InstanceIds=[instance_id])
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    return describe(ec2, instance_id)


def stop_and_wait(ec2, instance_id: str) -> Instance:
    """Stop the instance and block until EC2 reports it stopped."""
    ec2.stop_instances(InstanceIds=[instance_id])
    ec2.get_waiter("instance_stopped").wait(InstanceIds=[instance_id])
    return describe(ec2, instance_id)


# ---------------------------------------------------------------------------
# DuckDNS and the gateway's health endpoint
# ---------------------------------------------------------------------------

def resolve(domain: str) -> str | None:
    """The IP the DuckDNS name resolves to right now, or None if it does not resolve."""
    try:
        return socket.gethostbyname(f"{domain}.duckdns.org")
    except socket.gaierror:
        return None


def duckdns_update(domain: str, token: str, ip: str, api: str = DUCKDNS_API) -> None:
    """Point the domain at `ip`. DuckDNS answers a plain OK or KO.

    DuckDNS takes the token as a query parameter; that is its API, not a choice made here. The
    request goes over HTTPS and the URL is never printed.
    """
    query = urllib.parse.urlencode({"domains": domain, "token": token, "ip": ip})
    try:
        with urllib.request.urlopen(f"{api}/update?{query}", timeout=15) as response:
            body = response.read().decode().strip()
            status = response.status
    except urllib.error.HTTPError as ex:
        raise DnsError(f"DuckDNS refused the update (HTTP {ex.code}). Check DUCKDNS_TOKEN and DUCKDNS_DOMAIN.") from ex
    except urllib.error.URLError as ex:
        raise DnsError(f"Could not reach DuckDNS: {ex.reason}") from ex
    if status != 200 or body != "OK":
        # KO usually means a wrong token, or a domain this account does not own.
        raise DnsError(f"DuckDNS refused the update (HTTP {status}, body '{body}'). "
                       "Check DUCKDNS_TOKEN and DUCKDNS_DOMAIN.")


def wait_until_up(url: str, attempts: int, interval: float, sleep: Callable[[float], None] = time.sleep) -> bool:
    """Poll the gateway's public liveness URL until it answers 200."""
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            pass  # not up yet: DNS still old, the container starting, Caddy fetching a cert
        if attempt < attempts - 1:
            sleep(interval)
    return False


def dns_verdict(resolved: str | None, public_ip: str | None) -> str:
    if public_ip is None:
        return "instance has no public IP"
    if resolved is None:
        return "does not resolve"
    return "up to date" if resolved == public_ip else "STALE: run ec2ctl.py sync-dns"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@dataclass
class Context:
    """Everything a command touches, so tests can swap the network parts out."""
    ec2: object
    env: Mapping[str, str]
    out: TextIO | None = None   # None: whatever sys.stdout is when a message is printed
    resolve: Callable[[str], str | None] = resolve
    update_dns: Callable[[str, str, str], None] = duckdns_update
    wait_until_up: Callable[..., bool] = field(default=wait_until_up)

    def say(self, message: str) -> None:
        print(message, file=self.out or sys.stdout)

    def setting(self, name: str) -> str | None:
        value = self.env.get(name, "").strip()
        return value or None


def cmd_status(ctx: Context, instance_id: str) -> int:
    instance = describe(ctx.ec2, instance_id)
    ctx.say(f"instance   {instance.id}")
    ctx.say(f"state      {instance.state}")
    ctx.say(f"public IP  {instance.public_ip or '(none: stopped instances have no public IP)'}")
    domain = ctx.setting("DUCKDNS_DOMAIN")
    if domain:
        resolved = ctx.resolve(domain)
        ctx.say(f"DNS        {domain}.duckdns.org -> {resolved}  ({dns_verdict(resolved, instance.public_ip)})")
    return 0


def cmd_start(ctx: Context, instance_id: str) -> int:
    before = describe(ctx.ec2, instance_id)
    if before.running:
        ctx.say(f"Already running at {before.public_ip}")
        running = before
    else:
        ctx.say(f"Starting {instance_id} (from {before.state})...")
        running = start_and_wait(ctx.ec2, instance_id)
    ctx.say(f"Running. Public IP {running.public_ip}")

    if not sync_dns(ctx, running):
        return 1
    url = health_url(ctx)
    if url is None:
        return 0
    ctx.say(f"Waiting for the gateway at {url} ...")
    if ctx.wait_until_up(url, HEALTH_ATTEMPTS, HEALTH_INTERVAL_SECONDS):
        ctx.say("Gateway is up.")
        return 0
    ctx.say("The instance is running but the gateway did not answer yet. Check with: ec2ctl.py status")
    return 1


def cmd_stop(ctx: Context, instance_id: str, confirmed: bool) -> int:
    if not confirmed:
        # Stopping takes the public gateway offline and changes its IP on the next start, so it
        # must be asked for explicitly, never by a stray or half-typed command.
        ctx.say("This stops the instance and takes the gateway offline. Run: ec2ctl.py stop --yes")
        return 2
    if describe(ctx.ec2, instance_id).stopped:
        ctx.say("Already stopped.")
        return 0
    ctx.say(f"Stopping {instance_id}...")
    stop_and_wait(ctx.ec2, instance_id)
    ctx.say("Stopped. Its public IP is released; `ec2ctl.py start` will point DuckDNS at the new one.")
    return 0


def cmd_sync_dns(ctx: Context, instance_id: str) -> int:
    return 0 if sync_dns(ctx, describe(ctx.ec2, instance_id)) else 1


def sync_dns(ctx: Context, instance: Instance) -> bool:
    """Returns False only if an update was needed and failed."""
    domain, token = ctx.setting("DUCKDNS_DOMAIN"), ctx.setting("DUCKDNS_TOKEN")
    if not domain or not token:
        ctx.say("DUCKDNS_DOMAIN / DUCKDNS_TOKEN not set: skipping the DNS update.")
        return True
    if instance.public_ip is None:
        ctx.say(f"The instance has no public IP (state {instance.state}): nothing to point DNS at.")
        return True
    resolved = ctx.resolve(domain)
    if resolved == instance.public_ip:
        ctx.say(f"DuckDNS already points at {resolved}.")
        return True
    try:
        ctx.update_dns(domain, token, instance.public_ip)
    except DnsError as ex:
        ctx.say(str(ex))
        return False
    was = f" (was {resolved})" if resolved else ""
    ctx.say(f"DuckDNS updated: {domain}.duckdns.org -> {instance.public_ip}{was}")
    return True


def health_url(ctx: Context) -> str | None:
    explicit = ctx.setting("MDG_HEALTH_URL")
    if explicit:
        return explicit
    domain = ctx.setting("DUCKDNS_DOMAIN")
    return f"https://{domain}.duckdns.org/actuator/health/liveness" if domain else None


# ---------------------------------------------------------------------------

def run(args: argparse.Namespace, ctx: Context) -> int:
    instance_id = ctx.setting("MDG_INSTANCE_ID")
    if not instance_id:
        ctx.say("error: MDG_INSTANCE_ID is not set (e.g. export MDG_INSTANCE_ID=i-0abc123def4567890)")
        return 1
    commands = {
        "status": lambda: cmd_status(ctx, instance_id),
        "start": lambda: cmd_start(ctx, instance_id),
        "stop": lambda: cmd_stop(ctx, instance_id, args.yes),
        "sync-dns": lambda: cmd_sync_dns(ctx, instance_id),
    }
    try:
        return commands[args.command]()
    except (NoCredentialsError, PartialCredentialsError):
        ctx.say("error: no AWS credentials. Run `aws configure` (or `aws sso login`) for a user allowed to "
                "describe, start and stop this instance; see tools/ec2ctl/README.md.")
    except ClientError as ex:
        # The useful part of an AWS error is its code and message, e.g. UnauthorizedOperation.
        error = ex.response.get("Error", {})
        ctx.say(f"error: AWS refused the request: {error.get('Code')}: {error.get('Message')}")
    except LookupError as ex:
        ctx.say(f"error: {ex}")
    return 1


def parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ec2ctl.py", description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="state, public IP, and whether DuckDNS points at it")
    commands.add_parser("start", help="start, update DuckDNS, wait until the gateway answers")
    stop = commands.add_parser("stop", help="stop and wait until stopped")
    stop.add_argument("--yes", action="store_true", help="confirm: this takes the gateway offline")
    commands.add_parser("sync-dns", help="point DuckDNS at the current IP if it is stale")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse(sys.argv[1:] if argv is None else argv)
    # Checked before anything touches AWS, so a missing id is reported as that and not as
    # whatever AWS setup problem happens to come first.
    if not os.environ.get("MDG_INSTANCE_ID", "").strip():
        print("error: MDG_INSTANCE_ID is not set (e.g. export MDG_INSTANCE_ID=i-0abc123def4567890)")
        return 1
    try:
        # A fresh session, not boto3's module-level default one, which caches credentials for
        # the life of the process: each run resolves region and credentials from scratch.
        # AWS_REGION is passed explicitly: boto3 itself only reads AWS_DEFAULT_REGION, while
        # the AWS CLI v2, the Java SDK and this project's docs all use AWS_REGION.
        session = boto3.session.Session(region_name=os.environ.get("AWS_REGION") or None)
        ec2 = session.client("ec2")
    except NoRegionError:
        print("error: no AWS region. Set AWS_REGION (e.g. export AWS_REGION=eu-north-1) or run `aws configure`.")
        return 1
    return run(args, Context(ec2=ec2, env=os.environ))


if __name__ == "__main__":
    sys.exit(main())
