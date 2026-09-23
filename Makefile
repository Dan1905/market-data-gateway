# Convenience targets for building and running the gateway.
#
# Any JDK 21+ works; `mvn clean verify` on its own is fine too. These targets exist for
# the docker/topic/DLQ shortcuts further down, not because the build needs coddling.

SHELL := /bin/bash

MVN := mvn

COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help jdk toolchain verify test build run up down restart logs ps topics dlq replay clean

help:  ## Show this help
	@echo "market-data-gateway"
	@echo
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
	@echo

jdk:  ## Show the JDK this build will use
	@mvn -version | grep -E '^Java version|^Maven home'

toolchain:  ## One-time: register a JDK 21 toolchain in ~/.m2/toolchains.xml
	@J=$$(for c in /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
	                /Library/Java/JavaVirtualMachines/*21*/Contents/Home \
	                /usr/lib/jvm/*21* ; do \
	        if [ -x "$$c/bin/javac" ] && "$$c/bin/javac" -version 2>&1 | grep -q '^javac 21\.'; then echo "$$c"; break; fi; \
	      done); \
	if [ -z "$$J" ]; then echo "No JDK 21 found. Install it: brew install openjdk@21"; exit 1; fi; \
	mkdir -p $$HOME/.m2; \
	printf '%s\n' '<?xml version="1.0" encoding="UTF-8"?>' \
	  '<toolchains xmlns="http://maven.apache.org/TOOLCHAINS/1.1.0">' \
	  '  <toolchain>' '    <type>jdk</type>' \
	  '    <provides><version>21</version></provides>' \
	  "    <configuration><jdkHome>$$J</jdkHome></configuration>" \
	  '  </toolchain>' '</toolchains>' > $$HOME/.m2/toolchains.xml; \
	echo "Registered JDK 21 toolchain -> $$J"

verify:  ## Full build: unit tests, Testcontainers integration tests, coverage gate
	$(MVN) clean verify

test:  ## Unit tests only (fast, no Docker needed)
	$(MVN) clean test

build:  ## Package the jar, skipping integration tests
	$(MVN) clean package -DskipITs

run:  ## Run the gateway on the host (needs a broker on localhost:19092)
	$(MVN) spring-boot:run

up:  ## Start the whole stack in Docker (gateway + Redpanda + Console)
	$(COMPOSE) up -d --build
	@echo
	@echo "  Console      http://localhost:8090"
	@echo "  Contract     http://localhost:8080/asyncapi.html"
	@echo "  Control API  http://localhost:8080/swagger-ui.html"

down:  ## Stop the stack (keeps topic data)
	$(COMPOSE) down

restart:  ## Rebuild and restart just the gateway
	$(COMPOSE) up -d --build market-data-gateway

logs:  ## Tail the gateway logs
	$(COMPOSE) logs -f market-data-gateway

ps:  ## Show stack status and per-venue connection state
	@$(COMPOSE) ps --format '{{.Name}}\t{{.Status}}'
	@echo
	@curl -s localhost:8080/actuator/health/exchanges 2>/dev/null | python3 -m json.tool || echo "gateway not reachable"

topics:  ## Tail normalized events off the topic
	docker exec redpanda rpk topic consume normalized-market-data

dlq:  ## Show dead letters with their error headers
	docker exec redpanda rpk topic consume market-data-dlq --print-headers -o :end -n 20

replay:  ## Dry-run a DLQ replay
	@curl -s -X POST localhost:8080/api/v1/dlq/replay \
	  -H 'Content-Type: application/json' -d '{"dryRun":true}' | python3 -m json.tool

clean:  ## Remove build output and wipe all topic data
	$(MVN) clean
	$(COMPOSE) down -v
