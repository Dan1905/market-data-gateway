"""Tests for ec2ctl. Nothing here calls AWS or DuckDNS.

EC2 is played by moto, which implements the EC2 API in memory, so the real boto3 calls,
waiters and response shapes are exercised. DuckDNS and the gateway's health endpoint are a
local HTTP server.

    pip install -r requirements-dev.txt
    pytest
"""

from __future__ import annotations

import argparse
import io
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import boto3
import pytest
from moto import mock_aws

import ec2ctl


@pytest.fixture(autouse=True)
def no_real_aws(monkeypatch):
    # Belt and braces: even outside mock_aws, nothing may find real credentials or config.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-north-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.fixture
def ec2():
    with mock_aws():
        yield boto3.client("ec2", region_name="eu-north-1")


@pytest.fixture
def instance_id(ec2):
    image = ec2.describe_images(Owners=["amazon"])["Images"][0]["ImageId"]
    return ec2.run_instances(ImageId=image, MinCount=1, MaxCount=1, InstanceType="t3.micro")["Instances"][0]["InstanceId"]


class FakeDns:
    """Stands in for DNS resolution and the DuckDNS update call."""

    def __init__(self, resolves_to=None, fail=False):
        self.resolves_to = resolves_to
        self.fail = fail
        self.updates = []

    def resolve(self, domain):
        return self.resolves_to

    def update(self, domain, token, ip):
        if self.fail:
            raise ec2ctl.DnsError("DuckDNS refused the update (HTTP 200, body 'KO'). Check DUCKDNS_TOKEN.")
        self.updates.append((domain, token, ip))


def context(ec2, dns=None, healthy=True, **env):
    settings = {"MDG_INSTANCE_ID": "unset", "DUCKDNS_DOMAIN": "example-gateway", "DUCKDNS_TOKEN": "test-token"}
    settings.update(env)
    dns = dns or FakeDns()
    health_calls = []

    def wait_until_up(url, attempts, interval):
        health_calls.append(url)
        return healthy

    ctx = ec2ctl.Context(ec2=ec2, env=settings, out=io.StringIO(), resolve=dns.resolve,
                         update_dns=dns.update, wait_until_up=wait_until_up)
    ctx.health_calls = health_calls
    return ctx


def run(ctx, *argv):
    return ec2ctl.run(ec2ctl.parse(list(argv)), ctx)


# ---------------------------------------------------------------------------
# EC2 through boto3 (moto)
# ---------------------------------------------------------------------------

def test_describe_reads_state_and_public_ip(ec2, instance_id):
    instance = ec2ctl.describe(ec2, instance_id)

    assert instance.id == instance_id
    assert instance.running
    assert instance.public_ip  # moto assigns one, as EC2 does in a default VPC


def test_unknown_instance_is_a_clear_error(ec2, instance_id):
    ctx = context(ec2, MDG_INSTANCE_ID="i-0000000000000000f")

    assert run(ctx, "status") == 1
    assert "InvalidInstanceID" in ctx.out.getvalue()


def test_stop_then_start_goes_through_the_real_waiters(ec2, instance_id):
    stopped = ec2ctl.stop_and_wait(ec2, instance_id)
    assert stopped.stopped
    assert stopped.public_ip is None  # released on stop

    started = ec2ctl.start_and_wait(ec2, instance_id)
    assert started.running
    assert started.public_ip


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def test_status_flags_dns_pointing_at_an_old_address(ec2, instance_id):
    ctx = context(ec2, FakeDns(resolves_to="13.60.88.157"), MDG_INSTANCE_ID=instance_id)

    assert run(ctx, "status") == 0

    out = ctx.out.getvalue()
    assert "running" in out
    assert "STALE" in out


def test_start_from_stopped_updates_dns_and_waits_for_the_gateway(ec2, instance_id):
    ec2ctl.stop_and_wait(ec2, instance_id)
    dns = FakeDns(resolves_to="13.60.88.157")
    ctx = context(ec2, dns, MDG_INSTANCE_ID=instance_id)

    assert run(ctx, "start") == 0

    new_ip = ec2ctl.describe(ec2, instance_id).public_ip
    assert dns.updates == [("example-gateway", "test-token", new_ip)]
    assert ctx.health_calls == ["https://example-gateway.duckdns.org/actuator/health/liveness"]
    out = ctx.out.getvalue()
    assert "DuckDNS updated" in out and "was 13.60.88.157" in out and "Gateway is up" in out


def test_start_when_running_and_dns_correct_changes_nothing(ec2, instance_id):
    ip = ec2ctl.describe(ec2, instance_id).public_ip
    dns = FakeDns(resolves_to=ip)
    ctx = context(ec2, dns, MDG_INSTANCE_ID=instance_id)

    assert run(ctx, "start") == 0

    assert dns.updates == []
    assert "Already running" in ctx.out.getvalue()


def test_duckdns_refusal_fails_the_command_without_waiting_on_health(ec2, instance_id):
    ctx = context(ec2, FakeDns(resolves_to="13.60.88.157", fail=True), MDG_INSTANCE_ID=instance_id)

    assert run(ctx, "start") == 1

    assert "KO" in ctx.out.getvalue()
    assert ctx.health_calls == []


def test_stop_refuses_without_yes(ec2, instance_id):
    ctx = context(ec2, MDG_INSTANCE_ID=instance_id)

    assert run(ctx, "stop") == 2

    assert ec2ctl.describe(ec2, instance_id).running
    assert "stop --yes" in ctx.out.getvalue()


def test_stop_with_yes_stops(ec2, instance_id):
    ctx = context(ec2, MDG_INSTANCE_ID=instance_id)

    assert run(ctx, "stop", "--yes") == 0

    assert ec2ctl.describe(ec2, instance_id).stopped


def test_without_duckdns_settings_the_dns_step_is_skipped(ec2, instance_id):
    dns = FakeDns(resolves_to="13.60.88.157")
    ctx = context(ec2, dns, MDG_INSTANCE_ID=instance_id, DUCKDNS_TOKEN="")

    assert run(ctx, "sync-dns") == 0

    assert dns.updates == []
    assert "skipping the DNS update" in ctx.out.getvalue()


def test_instance_id_is_required_and_the_message_says_how(ec2):
    ctx = context(ec2, MDG_INSTANCE_ID="")

    assert run(ctx, "status") == 1
    assert "MDG_INSTANCE_ID" in ctx.out.getvalue()


def test_dns_verdicts():
    assert ec2ctl.dns_verdict("1.2.3.4", "1.2.3.4") == "up to date"
    assert ec2ctl.dns_verdict("1.2.3.4", "5.6.7.8").startswith("STALE")
    assert ec2ctl.dns_verdict(None, "5.6.7.8") == "does not resolve"
    assert ec2ctl.dns_verdict("1.2.3.4", None) == "instance has no public IP"


# ---------------------------------------------------------------------------
# Setup errors, through main() and real boto3 with no configuration at all
# ---------------------------------------------------------------------------

def test_missing_instance_id_is_reported_before_any_aws_setup_problem(monkeypatch, capsys):
    monkeypatch.delenv("AWS_DEFAULT_REGION")
    monkeypatch.delenv("MDG_INSTANCE_ID", raising=False)

    assert ec2ctl.main(["status"]) == 1
    assert "MDG_INSTANCE_ID" in capsys.readouterr().out


def test_aws_region_is_honoured_although_boto3_only_reads_aws_default_region(monkeypatch, capsys):
    # The documented setting is AWS_REGION (as for the AWS CLI and the Java SDK). boto3 on its
    # own ignores it, so without explicit handling the tool would say "no region" here.
    monkeypatch.delenv("AWS_DEFAULT_REGION")
    monkeypatch.setenv("AWS_REGION", "eu-north-1")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY")
    monkeypatch.setenv("MDG_INSTANCE_ID", "i-0abc123def4567890")

    assert ec2ctl.main(["status"]) == 1
    out = capsys.readouterr().out
    assert "no AWS region" not in out
    assert "no AWS credentials" in out   # it got past the region, to the next missing piece


def test_no_region_is_one_line_saying_what_to_do(monkeypatch, capsys):
    monkeypatch.delenv("AWS_DEFAULT_REGION")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("MDG_INSTANCE_ID", "i-0abc123def4567890")

    assert ec2ctl.main(["status"]) == 1
    assert "no AWS region" in capsys.readouterr().out


def test_no_credentials_is_one_line_saying_what_to_do(monkeypatch, capsys):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY")
    monkeypatch.setenv("MDG_INSTANCE_ID", "i-0abc123def4567890")

    assert ec2ctl.main(["status"]) == 1
    assert "no AWS credentials" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# DuckDNS and the health check, against a local HTTP server
# ---------------------------------------------------------------------------

class LocalServer:
    """A tiny HTTP server answering from a list of (status, body) responses, recording queries."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.queries = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.queries.append(urlparse(self.path).query)
                status, body = owner.responses.pop(0) if len(owner.responses) > 1 else owner.responses[0]
                self.send_response(status)
                self.end_headers()
                self.wfile.write(body.encode())

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("localhost", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://localhost:{self.server.server_port}"

    def close(self):
        self.server.shutdown()


def test_duckdns_update_sends_domain_token_and_ip():
    server = LocalServer([(200, "OK")])
    try:
        ec2ctl.duckdns_update("example-gateway", "tok/en+1", "13.61.200.5", api=server.url)
    finally:
        server.close()
    # The token is URL-encoded, so one containing / or + still reaches DuckDNS intact.
    assert server.queries == ["domains=example-gateway&token=tok%2Fen%2B1&ip=13.61.200.5"]


def test_duckdns_ko_is_an_error_naming_the_likely_cause():
    server = LocalServer([(200, "KO")])
    try:
        with pytest.raises(ec2ctl.DnsError, match="DUCKDNS_TOKEN"):
            ec2ctl.duckdns_update("example-gateway", "wrong", "13.61.200.5", api=server.url)
    finally:
        server.close()


def test_health_check_polls_until_the_gateway_answers():
    server = LocalServer([(503, ""), (503, ""), (200, "")])
    sleeps = []
    try:
        assert ec2ctl.wait_until_up(server.url, attempts=10, interval=10, sleep=sleeps.append)
    finally:
        server.close()
    assert len(server.queries) == 3
    assert sleeps == [10, 10]


def test_health_check_gives_up_after_its_attempts():
    server = LocalServer([(503, "")])
    try:
        assert not ec2ctl.wait_until_up(server.url, attempts=3, interval=1, sleep=lambda s: None)
    finally:
        server.close()


def test_stop_flag_parses():
    assert ec2ctl.parse(["stop", "--yes"]).yes
    assert not ec2ctl.parse(["stop"]).yes
    assert isinstance(ec2ctl.parse(["status"]), argparse.Namespace)
