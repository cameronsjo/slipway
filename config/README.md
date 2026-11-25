# Linux production configuration

These files document the strict, non-secret inputs for the controller and its
privileged host witness. They are inert examples: `example.invalid` and every
all-zero digest must be replaced by a reviewed artifact digest before a
manifest can start. Unknown fields and trailing JSON are rejected.

The controller runs as UID 65532 and receives only the shared Unix socket. It
does not receive the containerd socket, host `/proc`, LVM access, or the host
identity ledger. The root witness accepts only that controller UID and exposes
only process, storage, and capacity observations. Worker Pods receive neither
set of credentials or host mounts.

`container_rootfs_bytes` appears in both files. The controller charges two
20GiB rootfs devices per worker, and every capacity response must report the
same size before admission opens. The witness also checks separate digests for
the effective containerd runtime/snapshotter configuration, Kata configuration,
and Kata artifact manifest on every storage observation. Live qualification
must replace the example paths and hashes with the exact MS-A2 files verified
after the dedicated runtime is installed.
