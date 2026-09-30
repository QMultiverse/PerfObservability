# One image for every Hub service and for the ESS. Which one runs is the
# command, not the build — the design doc calls for one ESS image and one Helm
# chart, and the same holds for the Hub's stages.
#
#   docker build -t payment-hub:dev .
#   docker run payment-hub:dev hub edge
#   docker run payment-hub:dev ess.cli serve --mode functional

# ---------------------------------------------------------------- builder
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

# confluent-kafka and lxml ship manylinux wheels; the compiler is here only in
# case a transitive dependency does not.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# The stubs must exist before the package is installed: pyproject maps the
# hub_proto package at gen/hub_proto, and generated code is never committed.
COPY proto/ proto/
COPY scripts/gen_proto.py scripts/
RUN pip install "grpcio-tools>=1.68" "protobuf>=5.28" \
 && python scripts/gen_proto.py

COPY pyproject.toml ./
COPY libs/ libs/
COPY hub/ hub/
COPY ess/ ess/
RUN pip install -e . \
 && pip uninstall -y grpcio-tools

# ---------------------------------------------------------------- runtime
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    HUB_LOG_LEVEL=INFO \
    HUB_TRACKING_MODE=functional \
    HUB_METRICS_PORT=9464

# Nothing in the payment path needs root.
RUN useradd --create-home --uid 10001 hub

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder --chown=hub:hub /build/gen /app/gen
COPY --chown=hub:hub pyproject.toml ./
COPY --chown=hub:hub libs/ libs/
COPY --chown=hub:hub hub/ hub/
COPY --chown=hub:hub ess/ ess/
COPY --chown=hub:hub proto/ proto/
COPY --chown=hub:hub samples/ samples/
# create_topics.py runs as its own container at stack start-up.
COPY --chown=hub:hub scripts/ scripts/

RUN pip install --no-deps -e . && chown -R hub:hub /app

USER hub

# 8443 gRPC edge, 8080 status API, 9101 ESS, 9464 /metrics.
EXPOSE 8443 8080 9101 9464

# The metrics endpoint doubles as a liveness signal: a process serving /metrics
# has finished bootstrap. Overridden to none for the one-shot containers.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://localhost:' + os.environ.get('HUB_METRICS_PORT','9464') + '/metrics', timeout=2)"

ENTRYPOINT ["python", "-m"]
CMD ["hub", "edge"]
