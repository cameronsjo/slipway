# syntax=docker/dockerfile:1.7
#
# The controller binary is supplied through the named `controller` context.
# The pinned runner image contributes only the public CA bundle needed for the
# GitHub and Kubernetes HTTPS clients.
FROM myoung34/github-runner@sha256:590cdc6864129ed5a7799f86fba536f29690b9f2ae988ef8a6b7d8da7ae4596d AS certs

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=controller /runner-pool-linux-amd64 /usr/local/bin/runner-pool

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/runner-pool"]
