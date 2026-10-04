# syntax=docker/dockerfile:1.7
# The broker image plus the Prometheus JMX exporter java agent (Apache 2.0).
#
# The agent runs inside the broker JVM and serves /metrics on :9404: broker
# throughput, request and transaction-marker timings, log sizes and the JVM's
# own heap, pools and GC. Rules are in jmx-exporter.yml, mounted by compose.
# Built by compose.yaml; nothing to run by hand.
FROM apache/kafka:3.8.1

ADD --checksum=sha256:7d61f737fd661610ccc14aea79764faa1ea94a340cbc8f0029b3d2edea3d80c1 \
    --chmod=644 \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar \
    /opt/jmx/jmx_prometheus_javaagent.jar
