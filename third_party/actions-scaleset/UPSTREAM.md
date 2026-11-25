# Pinned actions/scaleset fork

`third_party/actions-scaleset` is vendored third-party source from
[`github.com/actions/scaleset`](https://github.com/actions/scaleset) at commit
`21ecccd60efbb5be696202631b79fe671c145821` (SDK pseudo-version
`v0.4.1-0.20260911130003-21ecccd60efb`). The production files, upstream root
tests, TLS fixtures, and MIT license are copied unchanged from that revision.

The only product extension is `resume.go`: a public constructor that issues the
SDK's existing administrative PATCH for a journaled session UUID, validates its
stable identity and queue-URL fingerprint, and publishes the native session
handle only after validation. It never POSTs a replacement session. The same
file adds `DeleteKnownMessageSession`, which sends the SDK's existing session
DELETE for a journaled UUID with no handle, so a close whose outcome was lost
can be retried (homelab#1235). It never PATCHes or POSTs. The
controller supplies its own no-retry, no-redirect HTTP client and response hook
because the extension does not weaken or replace those controller controls.

The pinned SDK mints an installation-wide token with an empty request body.
The controller's retryable-HTTP request hook replaces that body immediately
before transport with the one repository and `administration: write`. Native
SDK fixtures assert the transmitted JSON, and unexpected installation-token
targets are made untransportable. This stays outside the fork so the copied
upstream production-file hashes continue to detect SDK drift.

The normal module gate runs the recovery tests, the pinned-source hash test, and
the affected upstream create/poll/ACK/acquire tests. The remaining copied tests
stay available for pin-update review; they are outside the normal gate because
an unrelated macOS certificate test hard-codes platform error wording that
changed in the installed Go toolchain.

When updating the upstream pin, compare every copied upstream file against the
new exact commit, run the fork's tests, and re-review `resume.go` against the
private fields and refresh behavior in `session_client.go`. Do not update this
fork as an incidental dependency bump.
