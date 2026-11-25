# syntax=docker/dockerfile:1.7
#
# This trusted helper carries no GitHub or Kubernetes credential. Both inputs
# are CGO-free Go binaries. The host-tool binary is installed only under three
# approved names; it chroots into the read-only MS-A2 root before exec.
FROM scratch
COPY --from=witness /runner-pool-host-witness-linux-amd64 /usr/local/bin/runner-pool-host-witness
COPY --from=host-tool /runner-pool-host-tool-linux-amd64 /usr/local/bin/sjo-host-crictl
COPY --from=host-tool /runner-pool-host-tool-linux-amd64 /usr/local/bin/sjo-host-ctr

USER root
ENTRYPOINT ["/usr/local/bin/runner-pool-host-witness"]
