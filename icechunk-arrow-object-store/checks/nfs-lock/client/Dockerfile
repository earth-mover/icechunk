FROM debian:trixie-slim

RUN apt-get -qq update \
    && apt-get -qq install -y --no-install-recommends nfs-common util-linux bc \
    && rm -rf /var/lib/apt/lists/*

COPY probe.sh /probe.sh
ENTRYPOINT ["/bin/sh", "/probe.sh"]
