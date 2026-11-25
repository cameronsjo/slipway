# syntax=docker/dockerfile:1.7
#
# The two image inputs are digest-pinned in image-lock.json and checked by
# check-lock.sh. The named worker context contains the already-built,
# CGO-free runner-pool-worker-linux-amd64 binary.
FROM docker@sha256:7bf70dcb85c822e3fa6f2d332143e474b99147b9d7b39102b0cfe8b04a7dd61b AS docker
FROM myoung34/github-runner:2.338.2-ubuntu-noble@sha256:23e3a4a1da30de5b462ad0ae13d901288e803a688075c3b9b5bb79b7148c07fa AS runner

USER root

# Docker is both the privileged Kata sidecar's daemon payload and the
# non-root runner's client/buildx support. The daemon still receives its own
# container command and remains the only privileged container in the VM.
COPY --from=docker /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker /usr/local/bin/dockerd /usr/local/bin/dockerd
COPY --from=docker /usr/local/bin/dockerd-entrypoint.sh /usr/local/bin/dockerd-entrypoint.sh
COPY --from=docker /usr/local/bin/containerd-shim-runc-v2 /usr/local/bin/containerd-shim-runc-v2
COPY --from=docker /usr/local/bin/ctr /usr/local/bin/ctr
COPY --from=docker /usr/local/bin/runc /usr/local/bin/runc
COPY --from=docker /usr/local/bin/docker-init /usr/local/bin/docker-init
COPY --from=docker /usr/local/bin/docker-proxy /usr/local/bin/docker-proxy
COPY --from=docker /usr/local/libexec/docker/cli-plugins/docker-buildx /usr/local/libexec/docker/cli-plugins/docker-buildx
COPY --from=worker /runner-pool-worker-linux-amd64 /usr/local/bin/runner-pool-worker
COPY entrypoint.sh /usr/local/bin/slipway-bootstrap
# --chmod applies to the DIRECTORIES BuildKit creates for the destination too,
# not just the file. That gave /etc/runner-pool and .../bootstrap mode 0444 --
# no execute bit, so uid 1001 could not traverse them and the worker died with
# `cannot read public bootstrap CA` after a completely successful handshake.
# The RUN below puts the directories back to 0755 and then proves the result AS
# THE WORKER, because every check here had run as root, which ignores the
# missing traverse bit entirely.
COPY --chmod=0444 bootstrap-ca.pem /etc/runner-pool/bootstrap/ca.pem

RUN set -eux; \
    chmod 0755 /etc/runner-pool /etc/runner-pool/bootstrap; \
    test -x /actions-runner/bin/Runner.Listener; \
    test -d /actions-runner/externals; \
    test -x /usr/local/bin/runner-pool-worker; \
    test -s /etc/runner-pool/bootstrap/ca.pem; \
    test -x /usr/local/libexec/docker/cli-plugins/docker-buildx; \
    if getent group docker >/dev/null 2>&1; then groupmod --non-unique --gid 121 docker; else groupadd --system --non-unique --gid 121 docker; fi; \
    test "$(getent group docker | cut -d: -f3)" = 121; \
    id -u runner >/dev/null 2>&1 || useradd --uid 1001 --gid docker --create-home --shell /bin/bash runner; \
    usermod --gid docker runner; \
    test "$(id -u runner)" = 1001; \
    test "$(id -g runner)" = 121; \
    install -d -o 1001 -g 121 -m 0770 /home/runner /actions-runner/_work /tmp; \
    install -d -o root -g root -m 0755 /var/run /var/lib/docker; \
    chmod 0555 /usr/local/bin/slipway-bootstrap /usr/local/bin/runner-pool-worker; \
    runuser -u runner -- sh -eux -c '\
      test -x /usr/local/bin/slipway-bootstrap; \
      test -x /usr/local/bin/runner-pool-worker; \
      test -x /actions-runner/bin/Runner.Listener; \
      cd /actions-runner; \
      test -d /actions-runner/externals; \
      test -w /actions-runner/_work; \
      test -w /tmp; \
      test -s /etc/runner-pool/bootstrap/ca.pem; \
      head -c 1 /etc/runner-pool/bootstrap/ca.pem >/dev/null'

# No VOLUME is declared: the backend mounts fresh per-job workspace, Docker
# state, socket, temporary and support paths, and can independently attest
# their absence during inventory and cleanup.
ENV DOCKER_HOST=unix:///var/run/docker.sock \
    HOME=/home/runner \
    RUNNER_TOOL_CACHE=/actions-runner/_work/_tool \
    AGENT_TOOLSDIRECTORY=/actions-runner/_work/_tool
WORKDIR /actions-runner
USER 1001:121
ENTRYPOINT ["/usr/local/bin/slipway-bootstrap"]
