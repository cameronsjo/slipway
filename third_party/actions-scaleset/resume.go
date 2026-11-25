package scaleset

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/uuid"
)

// SessionResumeIdentity is non-secret identity committed before process loss.
// A missing session UUID or queue fingerprint cannot use this recovery path.
type SessionResumeIdentity struct {
	ScaleSetID  int
	Owner       string
	SessionID   uuid.UUID
	QueueSHA256 [32]byte
}

// ResumeMessageSessionClient refreshes an existing, positively identified
// session through the native administrative API. It never POSTs a replacement.
func (c *Client) ResumeMessageSessionClient(ctx context.Context, identity SessionResumeIdentity, options ...HTTPOption) (*MessageSessionClient, error) {
	if identity.ScaleSetID <= 0 || identity.Owner == "" || identity.SessionID == uuid.Nil || identity.QueueSHA256 == ([32]byte{}) {
		return nil, errors.New("known session identity and routing fingerprint required")
	}
	option := c.httpClientOption
	for _, apply := range options {
		apply(&option)
	}
	client := &MessageSessionClient{
		innerClient:  c,
		commonClient: newCommonClient(c.SystemInfo(), option),
		owner:        identity.Owner,
		scaleSetID:   identity.ScaleSetID,
	}
	var session RunnerScaleSetSession
	path := fmt.Sprintf("/%s/%d/sessions/%s", scaleSetEndpoint, identity.ScaleSetID, identity.SessionID.String())
	if err := client.doSessionRequest(ctx, http.MethodPatch, path, nil, http.StatusOK, &session); err != nil {
		return nil, errors.New("known session refresh failed; retain identity and reconcile")
	}
	u, err := url.Parse(session.MessageQueueURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		session.SessionID != identity.SessionID || session.OwnerName != identity.Owner || session.RunnerScaleSet == nil ||
		session.RunnerScaleSet.ID != identity.ScaleSetID || session.MessageQueueAccessToken == "" ||
		sha256.Sum256([]byte(session.MessageQueueURL)) != identity.QueueSHA256 {
		return nil, errors.New("refreshed session identity, credentials or routing rejected")
	}
	client.session.Store(&session)
	return client, nil
}

// DeleteKnownMessageSession sends the SDK's existing session DELETE for a
// journaled session UUID without first refreshing it, for a close whose
// earlier outcome was lost. It never PATCHes or POSTs. The returned error
// keeps the SDK's status classification (a 404 wraps NotFoundError); a caller
// that must tell this DELETE's own 404 from one on the admin-token refresh
// before it supplies an HTTP client that records the DELETE's status.
func (c *Client) DeleteKnownMessageSession(ctx context.Context, scaleSetID int, sessionID uuid.UUID, options ...HTTPOption) error {
	if scaleSetID <= 0 || sessionID == uuid.Nil {
		return errors.New("known scale set and session identity required")
	}
	option := c.httpClientOption
	for _, apply := range options {
		apply(&option)
	}
	client := &MessageSessionClient{
		innerClient:  c,
		commonClient: newCommonClient(c.SystemInfo(), option),
		scaleSetID:   scaleSetID,
	}
	return client.deleteMessageSession(ctx, scaleSetID, sessionID)
}
