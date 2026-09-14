# NFS-Ganesha serves NFSv4 from user space, so the check needs no kernel nfsd
# and no privileged server container.
FROM debian:trixie-slim

RUN apt-get -qq update \
    && apt-get -qq install -y --no-install-recommends nfs-ganesha nfs-ganesha-vfs \
    && rm -rf /var/lib/apt/lists/*

COPY ganesha.conf /etc/ganesha/ganesha.conf
# NFSv4 keeps lock and open state under /var/lib/nfs/ganesha. Without it
# the server faults on the first LOCK and the client reports EIO.
RUN mkdir -p /export /var/run/ganesha /var/lib/nfs/ganesha/v4recov

CMD ["/usr/bin/ganesha.nfsd", "-F", "-L", "STDOUT", "-f", "/etc/ganesha/ganesha.conf"]
