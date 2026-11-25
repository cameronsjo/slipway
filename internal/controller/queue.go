package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
)

const maxQueueBytes = 8 << 20

type sessionIdentity struct {
	id    uuid.UUID
	queue string // Transient routing information; never journaled or logged.
}

type pollContextKey struct{}

type pollCapture struct {
	queue     *QueueSession
	ctx       context.Context // Caller lifetime, not the HTTP body's timer context.
	envelope  *Envelope
	committed bool
	err       error
	// unauthorized is set when this Poll invocation observed a 401 on the
	// queue GET -- the SDK's own silent refresh-and-retry path. Read by
	// classifyPollOutcome; never journaled or logged on its own.
	unauthorized bool
	// quarantine is the message ID to acknowledge unapplied, set when this
	// refusal brought its count to refusalQuarantineThreshold. Zero otherwise.
	quarantine int
	// refusalReason, refusals and shape describe that decision for the WARN
	// line. shape is counts only, never a value.
	refusalReason string
	refusals      int
	shape         refusedShape
}

// PollOutcome buckets one Poll invocation's GitHub result for the per-cycle
// `polls=` diagnostic: an ordinary empty long-poll, a delivered message, an
// SDK-silent token refresh, an envelope the journal itself refused (and so
// never acknowledged), an envelope refused often enough to be acknowledged
// unapplied, or anything else Poll treated as a failure.
type PollOutcome string

const (
	PollEmpty       PollOutcome = "202"
	PollMessage     PollOutcome = "200"
	PollRefreshed   PollOutcome = "401"
	PollRefused     PollOutcome = "refused"
	PollQuarantined PollOutcome = "quarantined"
	PollOther       PollOutcome = "other"
)

// ErrEnvelopeRefused marks a poll whose durable commit the journal itself
// declined (envelope.validate, repository mismatch, redelivery conflict, and
// so on) -- distinct from a transport or storage fault, and distinct from
// "heard": the queue was never acknowledged, so GitHub still owes it.
var ErrEnvelopeRefused = errors.New("queue envelope refused by the journal; not acknowledged")

// ErrEnvelopeQuarantined marks a poll whose envelope was refused the same way
// refusalQuarantineThreshold times and then acknowledged WITHOUT being
// applied. It wraps ErrEnvelopeRefused, because the journal still declined it.
var ErrEnvelopeQuarantined error = &quarantinedError{}

type quarantinedError struct{}

func (*quarantinedError) Error() string {
	return "queue envelope refused repeatedly; acknowledged without applying it"
}
func (*quarantinedError) Unwrap() error { return ErrEnvelopeRefused }

// classifyPollOutcome decides which bucket one Poll call counts against,
// purely from what it is about to return plus what its wire exchange
// observed. An SDK-silent 401-then-refresh (capture.unauthorized) is the most
// informative outcome worth surfacing on its own, so it wins even when the
// retry that followed it succeeded -- the point of counting it is knowing a
// session needed a refresh mid-cycle, not what happened after. Anything Poll
// is about to return as an error is "other", because the eventual answer to
// "did the queue answer this cycle" is no, whatever the wire briefly showed.
// A quarantine is checked before a plain refusal because it wraps one.
func classifyPollOutcome(capture *pollCapture, envelope *Envelope, err error) PollOutcome {
	switch {
	case errors.Is(err, ErrEnvelopeQuarantined):
		return PollQuarantined
	case errors.Is(err, ErrEnvelopeRefused):
		return PollRefused
	case err != nil:
		return PollOther
	case envelope != nil:
		return PollMessage
	case capture != nil && capture.unauthorized:
		return PollRefreshed
	default:
		return PollEmpty
	}
}

// responseFence survives a malformed refresh without exposing its token-bearing
// response. Atomic because SDK refresh may run alongside a long poll.
type responseFence struct {
	failed         atomic.Bool
	patchUnsettled atomic.Bool
}

var githubQueueOrigins = []string{"https://broker.actions.githubusercontent.com"}

func validQueueURL(raw string, allowedOrigins []string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path == "" {
		return false
	}
	for _, origin := range allowedOrigins {
		expected, err := url.Parse(origin)
		if err == nil && expected.Scheme == "https" && expected.Host != "" &&
			u.Scheme == expected.Scheme && strings.EqualFold(u.Host, expected.Host) {
			return true
		}
	}
	return false
}

func (q *QueueSession) matchesSDK() bool {
	if q.sdk == nil || q.fence.failed.Load() || q.fence.patchUnsettled.Load() {
		return false
	}
	session := q.sdk.Session()
	return session.SessionID == q.identity.id && session.OwnerName == q.owner &&
		session.RunnerScaleSet != nil && session.RunnerScaleSet.ID == q.key.ScaleSetID &&
		session.MessageQueueURL == q.identity.queue && session.MessageQueueAccessToken != ""
}

func (q *QueueSession) validateSession(body []byte, refresh bool) (scaleset.RunnerScaleSetSession, error) {
	var session scaleset.RunnerScaleSetSession
	if err := json.Unmarshal(body, &session); err != nil {
		return session, errors.New("invalid session response")
	}
	if !validQueueURL(session.MessageQueueURL, q.queueOrigins) ||
		session.SessionID == uuid.Nil || session.OwnerName != q.owner || session.RunnerScaleSet == nil ||
		session.RunnerScaleSet.ID != q.key.ScaleSetID || session.MessageQueueAccessToken == "" {
		return session, errors.New("invalid session identity or routing")
	}
	// GitHub's session CREATE response carries statistics; its REFRESH (PATCH)
	// response does not. Measured on the first live daemon restart, 2026-09-20:
	// identity, owner, scale set and queue fingerprint all matched and the field
	// was simply absent, so requiring it fenced every restarted controller
	// forever. Creation still requires it; a refresh validates it when present.
	if session.Statistics == nil {
		if !refresh {
			return session, errors.New("session creation response carries no statistics")
		}
	} else if err := (Envelope{MessageID: 1, Statistics: *session.Statistics}).validate(); err != nil {
		return session, err
	}
	identity := sessionIdentity{id: session.SessionID, queue: session.MessageQueueURL}
	fingerprint := sha256.Sum256([]byte(session.MessageQueueURL))
	if refresh && (identity.id != q.identity.id || q.queueSHA256 == ([32]byte{}) || fingerprint != q.queueSHA256 ||
		(q.identity.queue != "" && identity.queue != q.identity.queue)) {
		return session, errors.New("session identity changed during refresh; reconcile before retry")
	}
	return session, nil
}

func (q *QueueSession) httpClient() *retryablehttp.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 0
	// A 503 is an ordinary response, never an unknown mutation retry. The parent
	// SDK's admin-auth client is separate and cannot overwrite this retry policy.
	client.CheckRetry = func(_ context.Context, _ *http.Response, err error) (bool, error) { return false, err }
	client.HTTPClient.Timeout = 70 * time.Second // Accommodate the broker's long poll.
	client.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		// ErrUseLastResponse would allow the SDK's outer StandardClient to follow.
		return errors.New("queue API redirect refused")
	}
	client.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, _ int) {
		if q.isSessionPatch(req) {
			// A request can reach GitHub and lose its response. It becomes settled
			// only after the guarded response and statistics commit complete.
			q.fence.patchUnsettled.Store(true)
		}
	}
	client.ResponseLogHook = q.captureResponse
	return client
}

func (q *QueueSession) isSessionPatch(req *http.Request) bool {
	if req == nil || req.URL == nil || req.Method != http.MethodPatch || q.identity.id == uuid.Nil {
		return false
	}
	path := fmt.Sprintf("/_apis/runtime/runnerscalesets/%d/sessions/%s", q.key.ScaleSetID, q.identity.id)
	return strings.HasSuffix(req.URL.Path, path)
}

type failedResponseBody struct{}

func (failedResponseBody) Read([]byte) (int, error) {
	return 0, errors.New("queue response rejected before SDK decoding")
}
func (failedResponseBody) Close() error { return nil }

// readResponse closes the original even on read failure. Replaying through a
// NopCloser must not hide an original Close failure or a truncated HTTP-200 body.
func readResponse(resp *http.Response) ([]byte, error) {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxQueueBytes+1))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || len(body) > maxQueueBytes {
		resp.Body = failedResponseBody{}
		return nil, errors.New("queue response incomplete or exceeds size limit")
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf}), nil
}

// captureResponse runs before the pinned SDK's decoder, which partitions events
// by type and ignores unknown types. Durable commit happens in this hook, before
// that lossy transformation. No token-bearing wire bytes enter the journal.
func (q *QueueSession) captureResponse(_ retryablehttp.Logger, resp *http.Response) {
	req := resp.Request
	sessionPath := fmt.Sprintf("/_apis/runtime/runnerscalesets/%d/sessions", q.key.ScaleSetID)
	isSessionPatch := q.isSessionPatch(req)
	capture, _ := req.Context().Value(pollContextKey{}).(*pollCapture)
	if req.Method == http.MethodDelete && q.identity.id != uuid.Nil && strings.HasSuffix(req.URL.Path, sessionPath+"/"+q.identity.id.String()) {
		q.closeStatus.Store(int32(resp.StatusCode))
	}
	// The queue GET is the only request this session issues with this method,
	// so a 401 observed here is always the SDK's silent refresh-and-retry
	// path (session_client.go's getMessage), never a session PATCH/POST. This
	// must be recorded before the readResponse below, which discards the
	// original response on any read failure -- the status is still whatever
	// GitHub sent even when the body could not be captured.
	if req.Method == http.MethodGet && capture != nil && capture.queue == q && resp.StatusCode == http.StatusUnauthorized {
		capture.unauthorized = true
	}
	body, err := readResponse(resp)
	fail := func(err error) {
		if isSessionPatch {
			q.fence.failed.Store(true)
		}
		resp.Body = failedResponseBody{}
		if capture != nil && capture.queue == q {
			capture.err = err
		}
	}
	if err != nil {
		fail(err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		if isSessionPatch {
			q.fence.failed.Store(true)
		}
		return // 202/401/error statuses retain the SDK's native meaning.
	}
	path := req.URL.Path
	if req.Method == http.MethodPost && strings.HasSuffix(path, sessionPath) {
		session, err := q.validateSession(body, false)
		if err != nil || q.identity.id != uuid.Nil {
			fail(errors.New("session creation identity rejected"))
			return
		}
		q.identity = sessionIdentity{id: session.SessionID, queue: session.MessageQueueURL}
		q.queueSHA256 = sha256.Sum256([]byte(session.MessageQueueURL))
		q.initialStatistics = *session.Statistics
		return
	}
	if req.Method == http.MethodPatch && strings.HasSuffix(path, sessionPath+"/"+q.identity.id.String()) {
		session, err := q.validateSession(body, true)
		if err != nil {
			fail(err) // Prevent the SDK from storing a foreign session and retrying.
			return
		}
		if q.identity.queue == "" {
			q.identity.queue = session.MessageQueueURL
		}
		// A successful refresh may be the only current demand observation before
		// the next 202. Save it without its token, with a lifetime independent of
		// the already-closed HTTP response. Statistics never settle lease identity.
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(req.Context()), persistenceTimeout)
		defer cancel()
		if session.Statistics != nil {
			if err := q.store.recordStatistics(saveCtx, q.key, *session.Statistics); err != nil {
				fail(errors.New("refreshed demand statistics could not be committed"))
				return
			}
		}
		q.fence.patchUnsettled.Store(false)
		return
	}
	if req.Method == http.MethodPost && strings.HasSuffix(path, fmt.Sprintf("/_apis/runtime/runnerscalesets/%d/acquirejobs", q.key.ScaleSetID)) {
		// The SDK ignores Count. Reject malformed success rather than treating it
		// as a definitive empty acquisition and later releasing its reservation.
		var result struct {
			Count *int     `json:"count"`
			Value *[]int64 `json:"value"`
		}
		if err := json.Unmarshal(body, &result); err != nil || result.Count == nil || result.Value == nil || *result.Count != len(*result.Value) {
			fail(errors.New("invalid acquisition result shape"))
		}
		return
	}
	if req.Method != http.MethodGet || capture == nil || capture.queue != q {
		fail(errors.New("unrecognized queue response"))
		return
	}
	u := *req.URL
	query := u.Query()
	query.Del("lastMessageId")
	u.RawQuery = query.Encode()
	if u.String() != q.identity.queue || !q.matchesSDK() || capture.envelope != nil {
		fail(errors.New("queue response does not match the active poll identity"))
		return
	}
	envelope, err := ParseEnvelope(body)
	if err != nil {
		// A body that fails to parse has no trusted envelope, so its message ID
		// comes from the one field that still decodes, if it does.
		if id, ok := wireMessageID(body); ok {
			q.countRefusal(capture, id, refusedShape{}, err)
		}
		fail(fmt.Errorf("%w: %w", ErrEnvelopeRefused, err))
		return
	}
	// Closing a timed HTTP response may cancel resp.Request.Context(). SQLite
	// must use the caller's still-live poll context, not that transport lifetime.
	if err := q.store.RecordEnvelope(capture.ctx, q.key, q.identity.id, envelope); err != nil {
		q.countRefusal(capture, envelope.MessageID, shapeOf(envelope), err)
		fail(fmt.Errorf("%w: %w", ErrEnvelopeRefused, err))
		return
	}
	capture.envelope, capture.committed = &envelope, true
}

// wireMessageID reads only messageId from a body that failed ParseEnvelope.
// No valid ID means the refusal cannot be counted, and the message keeps
// looping exactly as it did before quarantine existed.
func wireMessageID(body []byte) (int, bool) {
	var wire struct {
		MessageID int `json:"messageId"`
	}
	if err := json.Unmarshal(body, &wire); err != nil || wire.MessageID <= 0 {
		return 0, false
	}
	return wire.MessageID, true
}

// countRefusal records a deterministic refusal and, at the threshold, marks
// the message for quarantine. A transient failure (refusalReasonOf false) is
// never counted. A failure to count leaves the endless retry in place, which
// is the behaviour before quarantine existed, so it is logged and not fatal.
func (q *QueueSession) countRefusal(capture *pollCapture, messageID int, shape refusedShape, err error) {
	reason, deterministic := refusalReasonOf(err)
	if !deterministic {
		return
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(capture.ctx), persistenceTimeout)
	defer cancel()
	refusals, recordErr := q.store.RecordRefusal(saveCtx, q.key, q.identity.id, messageID, reason, shape)
	if recordErr != nil {
		slog.Warn("Could not count a queue envelope refusal; it will be retried without a count.",
			"repository_id", q.key.RepositoryID, "scale_set_id", q.key.ScaleSetID, "message_id", messageID, "reason", reason)
		return
	}
	if refusals >= refusalQuarantineThreshold {
		capture.quarantine, capture.refusalReason, capture.refusals, capture.shape = messageID, reason, refusals, shape
	}
}

// quarantineRefused acknowledges a message refused refusalQuarantineThreshold
// times without applying it. A failed DELETE returns the refusal itself and
// leaves the row un-acked, so the next redelivery counts again and retries.
// Quarantine does not record liveness: nothing was committed.
//
// The whole envelope is dropped, sibling events included; the WARN line and
// the row keep only its event count and per-type counts.
//
// Any refusal can have a durable messages row: an earlier delivery of the same
// ID committed and lost its ACK, and the redelivery was then refused for any
// reason (validation and the foreign-repository check run before the
// earlier-delivery lookup). That row is acknowledged BEFORE the DELETE. Its
// envelope was already applied, so acking early is safe, and a crash after the
// DELETE cannot leave the row unacknowledged and pin the cursor for good.
//
// If that ack fails, nothing is deleted and the next redelivery tries again.
// If the DELETE then fails, an ack this call made is reverted, so the cursor
// never passes a message GitHub still holds. If the revert fails too, the row
// stays acked with the message still queued; that window is two consecutive
// SQLite failures around one failed DELETE, far narrower than acking and
// hoping. It is logged at WARN, and the next redelivery quarantines again.
func (q *QueueSession) quarantineRefused(ctx context.Context, capture *pollCapture) error {
	id := capture.quarantine
	logState := []any{"repository_id", q.key.RepositoryID, "scale_set_id", q.key.ScaleSetID, "message_id", id, "reason", capture.refusalReason}
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	flipped, ackErr := q.store.AckIfPresent(ackCtx, q.key, q.identity.id, id)
	ackCancel()
	if ackErr != nil {
		slog.Warn("Could not acknowledge a refused message's earlier delivery; its quarantine waits for the next redelivery.", logState...)
		return capture.err
	}
	if delErr := q.sdk.DeleteMessage(ctx, id); delErr != nil {
		if flipped {
			revertCtx, revertCancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
			revertErr := q.store.RevertAcknowledged(revertCtx, q.key, q.identity.id, id)
			revertCancel()
			if revertErr != nil {
				slog.Warn("Quarantine DELETE failed and the earlier delivery's acknowledgment could not be reverted; the cursor may pass a message GitHub still holds.", logState...)
			}
		}
		return capture.err
	}
	slog.Warn("Quarantined a queue envelope refused "+strconv.Itoa(capture.refusals)+" times; acknowledged without applying it or any of its events.",
		"repository_id", q.key.RepositoryID, "scale_set_id", q.key.ScaleSetID, "message_id", id,
		"reason", capture.refusalReason, "refusals", capture.refusals,
		"events", capture.shape.events,
		"job_available", capture.shape.available, "job_assigned", capture.shape.assigned,
		"job_started", capture.shape.started, "job_completed", capture.shape.completed)
	quarantined := fmt.Errorf("%w: reason=%s message_id=%d", ErrEnvelopeQuarantined, capture.refusalReason, id)
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if markErr := q.store.MarkQuarantined(saveCtx, q.key, q.identity.id, id); markErr != nil {
		return errors.Join(quarantined, fmt.Errorf("the acknowledged quarantine could not be recorded: %w", markErr))
	}
	return quarantined
}

// Poll returns the original ordered envelope after durable receipt and ACK.
// maxCapacity advertises demand only; it never grants a physical slot. Callers
// must still use ReserveNext/Acquire and rotate advertisements across repos.
//
// A refused envelope is not acknowledged and returns ErrEnvelopeRefused, with
// one exception: the refusal that brings a message's same-kind count to
// refusalQuarantineThreshold acknowledges it unapplied and returns
// ErrEnvelopeQuarantined. Transient failures never count.
func (q *QueueSession) Poll(ctx context.Context, maxCapacity int) (envelope *Envelope, err error) {
	q.pollMu.Lock()
	defer q.pollMu.Unlock()
	q.gate.RLock()
	defer q.gate.RUnlock()
	capture := &pollCapture{queue: q, ctx: ctx}
	// Runs while pollMu is still held (defers unwind in reverse of declaration,
	// so this fires before the unlocks above), classifying the outcome this
	// call is about to return. No other Poll on this session can be in
	// flight, so LastPollOutcome is settled before the next caller could read
	// or overwrite it.
	defer func() { q.lastOutcome = classifyPollOutcome(capture, envelope, err) }()
	if maxCapacity < 0 || maxCapacity > q.store.config.Capacity-q.store.config.LegacySlots {
		err = errors.New("capacity advertisement exceeds platform qualification limit")
		return nil, err
	}
	if activeErr := q.active(ctx); activeErr != nil {
		return nil, activeErr
	}
	cursor, cursorErr := q.store.Cursor(ctx, q.key, q.identity.id)
	if cursorErr != nil {
		return nil, cursorErr
	}
	message, sdkErr := q.sdk.GetMessage(context.WithValue(ctx, pollContextKey{}, capture), cursor, maxCapacity)
	if capture.err != nil {
		if capture.quarantine > 0 && q.matchesSDK() {
			return nil, q.quarantineRefused(ctx, capture)
		}
		return nil, capture.err
	}
	if sdkErr != nil || !q.matchesSDK() {
		return nil, errors.New("queue polling failed; no acknowledgment sent")
	}
	if message == nil && capture.envelope == nil {
		return nil, nil
	}
	if message == nil || !capture.committed || capture.envelope == nil || message.MessageID != capture.envelope.MessageID {
		return nil, errors.New("SDK result lacks matching durable wire receipt; do not acknowledge")
	}
	if delErr := q.sdk.DeleteMessage(ctx, message.MessageID); delErr != nil {
		return capture.envelope, errors.New("queue acknowledgment outcome unknown; durable envelope retained")
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if markErr := q.store.MarkAcknowledged(saveCtx, q.key, q.identity.id, message.MessageID); markErr != nil {
		return capture.envelope, markErr
	}
	return capture.envelope, nil
}
