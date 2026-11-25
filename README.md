# slipway

Ephemeral, isolated self-hosted GitHub Actions runner pools for Linux (Kubernetes + Kata Containers) and macOS (Tart VMs + pf).

Field guide, architecture, and operating notes: [docs/README.md](docs/README.md).

## What slipway does

- **One VM per job, zero credential leaks:** Each Linux job runs in a fresh Kata Containers VM (`devmapper` thin-pool snapshotter, dedicated bridge, nftables egress guard); each macOS job runs in a fresh ephemeral Tart VM behind a verified `pf` anchor.
- **Single-use JIT bootstrap:** GitHub App private keys and reusable registration tokens never leave the trusted controller process. Workers receive a one-time bootstrap capability over an isolated attach stream (`SLIPWAY_BOOTSTRAP_*`) and exchange it over mTLS/pinned-CA HTTPS for a single-use GitHub Actions JIT runner config. See [internal/bootstrap/CONTRACT.md](internal/bootstrap/CONTRACT.md).
- **Host-witnessed capacity & runtime attestation:** On Linux, a separate `scratch`-based `host-witness` sidecar authenticates the controller over Unix peer credentials, verifies host CRI/QEMU process identity, thin-pool headroom, and pinned runtime/firewall fingerprints before admitting any worker.
- **Durable queue recovery:** Uses a vendored revision of GitHub's `actions/scaleset` (`third_party/actions-scaleset/`) with persistence-before-ACK in SQLite, session resume across restarts, and automatic deaf-session recovery.

## Repository layout

- `cmd/runner-pool` — Controller daemon (`discover`, `probe`, `recover`, `daemon`)
- `cmd/runner-pool-host-witness` — Linux host-side capacity, storage, and fingerprint witness
- `cmd/runner-pool-host-tool` — Allowlisted host-tool launcher for the `scratch` witness image
- `cmd/runner-pool-worker` — In-VM HTTPS JIT bootstrap launcher for Linux workers
- `cmd/runner-pool-guest-launch` — In-VM bootstrap launcher for macOS Tart workers
- `internal/` — Controller core, GitHub App auth, bootstrap broker, admission policy, and Kubernetes / Tart backends
- `config/` — Strict, inert example configurations for Linux and macOS ([config/README.md](config/README.md))
- `image/` — Source-only Dockerfiles and reproducible OCI build inputs ([image/README.md](image/README.md); bring your own bootstrap CA)
- `docs/` — Architecture diagrams, isolation model, capacity accounting, and operations guide ([docs/README.md](docs/README.md))

## Build & verify

```sh
make check test build
```

Or directly with Go:

```sh
go vet ./...
go test -race -count=1 ./...
```

## Distribution

`slipway` is distributed **source-only**. Operators build their own container images from a pinned ref and supply their own private bootstrap CA (`image/linux/bootstrap-ca.pem`) so no shared trust root is ever published.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for third-party attribution (including MIT-licensed `third_party/actions-scaleset/`).
