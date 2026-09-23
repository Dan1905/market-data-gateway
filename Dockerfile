# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1 — build
#
# JDK 21 matches maven.compiler.release, so what CI builds and what ships here are
# byte-for-byte the same target. Any JDK 21+ compiles this correctly.
# =============================================================================
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# The build declares a JDK 21 toolchain so that compilation and tests run on the same JVM
# everywhere - laptop, CI and this image. That needs a toolchains.xml naming a JDK 21;
# this image already IS one, so point the toolchain at its own JAVA_HOME.
#
# Written to /opt, NOT ~/.m2: the dependency layer below mounts a build cache over
# /root/.m2, which would shadow anything written there in an earlier layer. MAVEN_ARGS
# (Maven 3.9+) then hands the file to every mvn invocation.
RUN mkdir -p /opt && printf '%s\n' \
    '<?xml version="1.0" encoding="UTF-8"?>' \
    '<toolchains xmlns="http://maven.apache.org/TOOLCHAINS/1.1.0">' \
    '  <toolchain>' \
    '    <type>jdk</type>' \
    '    <provides><version>21</version></provides>' \
    "    <configuration><jdkHome>$JAVA_HOME</jdkHome></configuration>" \
    '  </toolchain>' \
    '</toolchains>' > /opt/toolchains.xml
ENV MAVEN_ARGS="-t /opt/toolchains.xml"

# Dependencies resolve in their own layer, keyed on the POM alone. Editing a source
# file then costs a recompile, not a re-download of the entire dependency tree.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline

COPY src ./src

# Integration tests need a Docker daemon for Testcontainers, which is not available
# inside a build stage, so they are always skipped here.
#
# Unit tests run by DEFAULT, so a plain `docker compose build` on a laptop still catches a
# broken change. CI passes SKIP_TESTS=true because its test job has already run the full
# suite — including the integration tests — against the same commit. Running them twice
# added ~55s to every pipeline for no extra signal.
ARG SKIP_TESTS=false
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q clean package -DskipITs -Dmaven.test.skip=${SKIP_TESTS}

# Unpack the fat jar into layers. Dependencies change rarely and application classes
# change every commit, so splitting them means a redeploy ships a few hundred KB
# rather than the whole ~40MB archive.
#
# This produces extracted/dependencies/lib/*.jar and extracted/application/<name>.jar,
# where the application jar's manifest Class-Path points at a sibling lib/ directory.
# Renaming it here keeps the ENTRYPOINT free of the project version.
RUN java -Djarmode=tools -jar target/*.jar extract --layers --destination extracted \
    && mv extracted/application/*.jar extracted/application/app.jar

# =============================================================================
# Stage 2 — runtime
# =============================================================================
FROM eclipse-temurin:21-jre-alpine AS runtime

# Run unprivileged. Nothing in this service needs root, and a container escape from
# a market data gateway should not land on a root shell.
RUN addgroup -S gateway && adduser -S -G gateway gateway

WORKDIR /app

# Order matters for caching: dependencies first (they change on a POM edit), the
# application jar last (it changes on every commit).
COPY --from=build --chown=gateway:gateway /build/extracted/dependencies/ ./
COPY --from=build --chown=gateway:gateway /build/extracted/application/ ./

USER gateway

EXPOSE 8080

# -----------------------------------------------------------------------------
# JVM sizing for a 1 GB box (t2.micro / t3.micro).
#
# MaxRAMPercentage, not -Xmx: the JVM reads the cgroup limit, so the heap tracks
# whatever `mem_limit` the container was given instead of being pinned to a number
# that silently becomes wrong when the limit changes.
#
# SerialGC because the alternatives cost more than they return here. G1 reserves
# region metadata and runs concurrent threads that a 2-vCPU burstable instance does
# not have to spare; at a ~250MB heap its pause advantage is not measurable, while
# its footprint is. Revisit if the heap grows past ~512MB or the instance gets more
# cores.
#
# ExitOnOutOfMemoryError because a gateway limping along after an OOM silently drops
# market data. Failing fast lets the orchestrator restart it and lets the DLQ and the
# venue's own replay cover the gap.
# -----------------------------------------------------------------------------
ENV JAVA_OPTS="\
-XX:MaxRAMPercentage=70 \
-XX:+UseSerialGC \
-XX:MaxMetaspaceSize=160m \
-XX:+ExitOnOutOfMemoryError \
-XX:+AlwaysActAsServerClassMachine \
-Djava.security.egd=file:/dev/./urandom \
-Duser.timezone=UTC"

HEALTHCHECK --interval=30s --timeout=3s --start-period=45s --retries=3 \
    CMD wget -qO- http://localhost:8080/actuator/health/liveness || exit 1

# exec form via sh so JAVA_OPTS expands, while PID 1 stays the JVM — without `exec`
# the shell would be PID 1 and SIGTERM would never reach Spring's graceful shutdown.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
