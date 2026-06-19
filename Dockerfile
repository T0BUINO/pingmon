FROM alpine:3.22

ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION=latest
ARG REPO=T0BUINO/pingmon

RUN apk add --no-cache ca-certificates curl tar

WORKDIR /opt/pingmon

RUN set -eux; \
    case "${TARGETARCH}${TARGETVARIANT}" in \
      amd64) suffix="linux-amd64" ;; \
      386) suffix="linux-386" ;; \
      arm64) suffix="linux-arm64" ;; \
      armv6) suffix="linux-armv6" ;; \
      armv7) suffix="linux-armv7" ;; \
      riscv64) suffix="linux-riscv64" ;; \
      loong64) suffix="linux-loong64" ;; \
      *) echo "unsupported target: ${TARGETARCH}${TARGETVARIANT}" >&2; exit 1 ;; \
    esac; \
    if [ "${VERSION}" = "latest" ]; then \
      url="https://github.com/${REPO}/releases/latest/download/pingmon-${suffix}.tar.gz"; \
    else \
      url="https://github.com/${REPO}/releases/download/${VERSION}/pingmon-${suffix}.tar.gz"; \
    fi; \
    curl -fsSL "$url" -o /tmp/pingmon.tar.gz; \
    tar -xzf /tmp/pingmon.tar.gz --strip-components=1 -C /opt/pingmon; \
    rm /tmp/pingmon.tar.gz; \
    chmod +x /opt/pingmon/supervisor /opt/pingmon/agent; \
    mkdir -p /opt/pingmon/data

ENV PATH="/opt/pingmon:${PATH}"

EXPOSE 8080
VOLUME ["/opt/pingmon/data"]

ENTRYPOINT ["/opt/pingmon/supervisor"]
CMD ["-config", "/opt/pingmon/configs/supervisor.toml"]
