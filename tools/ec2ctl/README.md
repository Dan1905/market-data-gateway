# ec2ctl

A small Python script, built on **boto3** (the AWS SDK for Python), for the EC2 instance the
gateway runs on. It runs on your laptop, never in the service.

```
ec2ctl.py status        state, public IP, and whether DuckDNS points at it
ec2ctl.py start         start, wait until running, point DuckDNS at the new IP, wait until the gateway answers
ec2ctl.py stop --yes    stop and wait until stopped
ec2ctl.py sync-dns      point DuckDNS at the current IP if it is stale
```

## Why it exists

Stopping the instance when you are not using it saves free-tier hours. But AWS releases an
instance's automatically assigned public IP when it stops and gives it a new one when it
starts, so the DuckDNS name is left pointing at a dead address. That broke a deploy once.
An Elastic IP would keep the address fixed, but AWS bills an Elastic IP while the instance
is stopped. `ec2ctl.py start` instead starts the instance and moves the name to wherever it
landed.

## Install

Python 3.10 or newer, one dependency:

```bash
cd tools/ec2ctl
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

## Set up AWS access (once)

The script uses boto3's normal credential lookup, so whatever `aws configure` (or
`aws sso login`) sets up is what it uses. No keys go in this repository.

Give the user or role you sign in with this policy, replacing the placeholders. `Describe`
cannot be limited to one instance in IAM; starting and stopping are limited to this one:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "ec2:DescribeInstances", "Resource": "*" },
    {
      "Effect": "Allow",
      "Action": ["ec2:StartInstances", "ec2:StopInstances"],
      "Resource": "arn:aws:ec2:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID>"
    }
  ]
}
```

## Configure

Environment variables only, for example in your shell profile:

```bash
export AWS_REGION=eu-north-1
export MDG_INSTANCE_ID=i-0abc123def4567890
export DUCKDNS_DOMAIN=your-subdomain        # the part before .duckdns.org
export DUCKDNS_TOKEN=...                    # from your duckdns.org account page
```

`DUCKDNS_*` are optional: without them `start` and `status` still work and the DNS step is
skipped. `MDG_HEALTH_URL` overrides the URL `start` waits on, which defaults to
`https://<DUCKDNS_DOMAIN>.duckdns.org/actuator/health/liveness`.

`AWS_REGION` is honoured although boto3 on its own reads only `AWS_DEFAULT_REGION`; either
works, as does a region in `~/.aws/config`.

## Use

```bash
./ec2ctl.py status
./ec2ctl.py stop --yes
./ec2ctl.py start
```

`stop` refuses without `--yes`, because it takes the public gateway offline. If AWS access
is missing, the script says so in one line (no instance id, no region, or no credentials).

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

EC2 is played by [moto](https://github.com/getmoto/moto), which implements the EC2 API in
memory, so the real boto3 calls, waiters and response shapes are exercised (start and stop
go through boto3's `instance_running` / `instance_stopped` waiters). DuckDNS and the health
check run against a local HTTP server. Nothing calls AWS or DuckDNS: the suite also passes
with every connection to `amazonaws.com` refused.
