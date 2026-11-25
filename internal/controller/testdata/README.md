# Controller wire fixtures

**The key sets here are measured. Every value is synthetic.**

Both files reproduce the *shape* GitHub sent to the macOS pilot on 2026-09-20, read
from a throwaway diagnostic build that logged key names and booleans only, never
values (`docs/evidence/2026-09-20-runner-pool-first-live-job/README.md`, issue
[homelab#824](https://github.com/cameronsjo/homelab/issues/824)). No captured
value was carried across: owners, repositories, job IDs, URLs and tokens are all
invented and obviously fake. Nothing here is a credential.

Every controller test before these built its envelope and session bodies by hand,
which is why neither defect was caught before a real job ran.

## `live-jobassigned-2026-09-20.json`

One queue envelope carrying exactly the fourteen keys measured on the live
`JobAssigned` event: `eventName`, `finishTime`, `jobDisplayName`, `jobId`,
`jobWorkflowRef`, `messageType`, `ownerName`, `queueTime`, `repositoryName`,
`requestLabels`, `runnerAssignTime`, `runnerRequestId`, `scaleSetAssignTime`,
`workflowRunId`. `runnerRequestId` is present and **zero**; every other value is
non-zero. There is no preceding `JobAvailable`: the broker assigned the job to
the scale set directly and identifies it by the string `jobId`. Upstream reports
the same shape in actions/scaleset#107.

## `live-session-refresh-2026-09-20.json`

A session **refresh** (`PATCH`) response body. It has **no `statistics` field** —
that is the measured shape, and requiring it fenced every restarted controller.
A session *creation* response does carry statistics, and the controller still
requires it there.
