# GitHub scale-set API behavior

The pool talks to GitHub through repository-scoped runner scale sets and their long-poll message queue, using a pinned copy of GitHub's `actions/scaleset` client. What follows was measured against github.com, not taken from documentation.

## A job arrives as a lone `JobAssigned` with request ID 0

- **Expected:** GitHub sends `JobAvailable` with a positive `runnerRequestId`, the controller calls `AcquireJobs`, then GitHub confirms with `JobAssigned`.
- **Actually:** on github.com a job arrives as one `JobAssigned` event. There is no `JobAvailable` and nothing to acquire. `runnerRequestId` is present and is `0`. The job is identified only by a string `jobId`. `JobStarted` and `JobCompleted` for the same job also carry request ID 0 and the same `jobId`. `AcquireJobs` still exists because GitHub Enterprise Server uses the other shape.
- **Cost:** anything keyed on the integer request ID works in hand-written tests and fails on the first real job. Every synthetic test here passed until a real payload was captured.
- **Code:** job identity is the request ID when positive, otherwise `job:<jobId>` lowercased (`internal/controller/inbox.go:68-80`). The `JobAssigned` branch is `internal/controller/inbox.go:127`. The reservation path self-certifies acceptance for request ID 0 (`internal/controller/allocator.go:303`).
- **Status:** `fixed`. The upstream client's reference listener ignores `JobAssigned` and scales from statistics instead; an upstream issue describing the same shape was unanswered when measured.

## A broker assignment cannot be re-confirmed after a restart

- **Expected:** after a restart the controller can ask GitHub whether an assignment it journaled is still live.
- **Actually:** there is no such API. `AcquireJobs` needs a positive request ID, and an acknowledged message is never redelivered because the durable cursor moved past it. The only confirmation is a fresh envelope naming the same job: a reassignment retry, or its completion. An assigned job nobody picks up is cancelled and reassigned, up to three times, and there is no un-assign call.
- **Cost:** a controller that trusts its pre-restart `assigned` rows mints runners for jobs GitHub already rerouted or finished.
- **Code:** inherited assignments become `assigned-stale` and are never minted a runner (`internal/controller/inbox.go:44-58`). They still count as demand while young, so the pool keeps advertising and invites the reassignment that re-confirms them (`internal/controller/advertisement.go:310-320`).
- **Status:** `fixed`.

## Advertising capacity only asks for work

- **Expected:** advertising capacity on a scale set is enough for jobs to flow.
- **Actually:** advertising only tells GitHub how many jobs you would accept. A worker starts only when an assignment envelope arrives. GitHub assigns only to a scale set that is currently advertising, so a repository advertised at zero never receives its queued jobs.
- **Cost:** a planner that advertises zero for a repository with queued work leaves that work queued forever, while the pool looks idle.
- **Code:** `internal/controller/advertisement.go:310-320` and the planner below it.
- **Status:** `by design` (GitHub's model). See [Capacity and scheduling](capacity-and-scheduling.md) for how the planner handles it.

## A session refresh carries no statistics

- **Expected:** refreshing a queue session returns the same queue statistics a create does.
- **Actually:** a session create response carries `statistics`. A refresh (`PATCH`) response does not. After a resume, statistics are frozen at whatever the previous process last saw. A long-poll that finds nothing returns `202` with no envelope, so statistics also stop updating whenever nothing happens.
- **Cost:** a planner that trusts statistics gives slots to frozen numbers. On one day a frozen non-zero count took the only slot every sweep while the repository with real queued jobs got none for 100 minutes.
- **Code:** freshness rules in `internal/controller/advertisement.go:95-185` and `classifyStatistics` at `internal/controller/advertisement.go:285-303`.
- **Status:** `fixed`.

## The upstream client cannot resume a known session

- **Expected:** after a controller restart, the client reattaches to the queue session it already owns.
- **Actually:** the pinned upstream client has no public way to resume a known session.
- **Cost:** every restart creates new sessions and loses in-flight message state.
- **Code:** the vendored client adds one reviewed file, a resume constructor (`third_party/actions-scaleset/UPSTREAM.md:9`).
- **Status:** `fixed` locally; not upstream.

## An empty poll looks like a successful poll

- **Expected:** a dead queue session produces an error.
- **Actually:** `Poll` returns nothing for an empty long-poll (`internal/controller/queue.go:336`). A dead session and an idle one look the same. In one outage every session was silent for about nine hours while GitHub held queued jobs, and the controller logged a normal-looking cycle every minute.
- **Cost:** jobs queue with no error anywhere. Cancelling and re-running them does nothing, because the jobs were never the problem.
- **Code:** a liveness check exits the process when no subscription has delivered an envelope for `-session-deaf-after` (default 30 minutes, `cmd/runner-pool/runtime.go:39-44`, exit at `cmd/runner-pool/runtime.go:460-465`), so the supervisor restarts it with fresh sessions.
- **Status:** `fixed`, with a limit: a restart cannot help when the silence is caused by the planner advertising zero to the repositories that have work. See [Capacity and scheduling](capacity-and-scheduling.md).

## Repository-scoped tokens echo an extra permission

- **Expected:** a token minted with `permissions: {actions: read}` and a repository list reports exactly that grant.
- **Actually:** GitHub adds `metadata: read` to any repository-scoped token and echoes it in the mint response.
- **Cost:** a strict "exactly what I asked for" check rejects every token GitHub mints.
- **Code:** `implicitGrantPermissions` allows exactly `metadata: read` and nothing else (`internal/githubapp/queued_jobs.go:354-357`); the full check is `verifyActionsReadGrant` (`internal/githubapp/queued_jobs.go:364`).
- **Status:** `fixed`.

## Queued jobs can be read without the queue

- **Expected:** the queue session is the only way to learn a repository has waiting work.
- **Actually:** the REST API lists queued workflow jobs per repository, including the runner labels they want. It sees jobs the queue never announced, such as jobs queued while sessions were deaf.
- **Cost:** without it, demand for a never-advertised repository is invisible.
- **Code:** the controller reads queued jobs with a short-lived, repository-scoped `actions: read` token, verifies the grant, and revokes it after each read (`internal/githubapp/queued_jobs.go`). A fresh reading raises demand to at least one slot (`internal/controller/rest_demand.go:39-51`, used at `internal/controller/advertisement.go:166`).
- **Status:** `fixed`. The read costs a few hundred API calls an hour at about 50 repositories.
