# ec2ctl

A small command-line tool, built on the **AWS SDK for Java v2**, for the EC2 instance the
gateway runs on. It runs on your laptop, never in the service.

```
ec2ctl status        state, public IP, and whether DuckDNS points at it
ec2ctl start         start, wait until running, point DuckDNS at the new IP, wait until the gateway answers
ec2ctl stop --yes    stop and wait until stopped
ec2ctl sync-dns      point DuckDNS at the current IP if it is stale
```

## Why it exists

Stopping the instance when you are not using it saves free-tier hours. But AWS releases an
instance's automatically assigned public IP when it stops and gives it a new one when it
starts, so the DuckDNS name is left pointing at a dead address. That broke a deploy once.
An Elastic IP would keep the address fixed, but AWS bills an Elastic IP while the instance
is stopped. `ec2ctl start` instead starts the instance and moves the name to wherever it
landed.

## Build

Needs JDK 21 registered as a Maven toolchain (`make toolchain` in the repository root does it).

```bash
mvn -q -f tools/ec2ctl/pom.xml package
```

The result is one runnable jar, `tools/ec2ctl/target/ec2ctl.jar`.

## Set up AWS access (once)

The tool uses the SDK's normal credential lookup, so whatever `aws configure` (or
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

## Use

```bash
java -jar tools/ec2ctl/target/ec2ctl.jar status
java -jar tools/ec2ctl/target/ec2ctl.jar stop --yes
java -jar tools/ec2ctl/target/ec2ctl.jar start
```

`stop` refuses without `--yes`, because it takes the public gateway offline.

If AWS access is missing, the tool says so in one line (no region, or no credentials) rather
than printing the SDK's provider chain.

## Tests

`mvn -f tools/ec2ctl/pom.xml test`: the EC2 calls run against a mocked SDK client (right
requests, in the right order, waiting for `running` / `stopped`), and DuckDNS and the health
check against a local HTTP server. Nothing calls AWS or DuckDNS.
