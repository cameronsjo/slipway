package scaleset

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/actions/scaleset/internal/testserver"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
)

// DeleteKnownMessageSession is the fork's handle-free session DELETE
// (homelab#1167). It must send exactly one DELETE to
// /{scaleSetEndpoint}/{set}/sessions/{uuid} and nothing else, keep the SDK's
// 404 classification, and refuse an unidentified session before any request.
func TestResumeExtensionDeleteKnownMessageSession(t *testing.T) {
	id := uuid.New()
	for name, tc := range map[string]struct {
		status  int
		wantErr error
	}{
		"deleted":      {http.StatusNoContent, nil},
		"already gone": {http.StatusNotFound, NotFoundError},
	} {
		t.Run(name, func(t *testing.T) {
			var deletes, others atomic.Int32
			server := testserver.NewUnstarted(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					others.Add(1)
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				deletes.Add(1)
				if want := fmt.Sprintf("/%s/17/sessions/%s", scaleSetEndpoint, id); !strings.HasSuffix(r.URL.Path, want) {
					t.Errorf("DELETE path %q does not end in %q", r.URL.Path, want)
				}
				if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
					t.Error("DELETE did not use administrative auth")
				}
				w.WriteHeader(tc.status)
			}))
			server.StartTLS()
			client := deleteTestClient(t, server.URL, server.Certificate())
			err := client.DeleteKnownMessageSession(context.Background(), 17, id, WithRetryableHTTPClint(noRetryHTTP()))
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("err = %v, want nil", err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if deletes.Load() != 1 || others.Load() != 0 {
				t.Fatalf("DELETEs = %d, other session requests = %d, want 1 and 0", deletes.Load(), others.Load())
			}
		})
	}
	for name, tc := range map[string]struct {
		scaleSetID int
		sessionID  uuid.UUID
	}{
		"nil session UUID": {17, uuid.Nil},
		"zero scale set":   {0, id},
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := testserver.NewUnstarted(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			server.StartTLS()
			client := deleteTestClient(t, server.URL, server.Certificate())
			if err := client.DeleteKnownMessageSession(context.Background(), tc.scaleSetID, tc.sessionID, WithRetryableHTTPClint(noRetryHTTP())); err == nil {
				t.Fatal("an unidentified session DELETE was accepted")
			}
			if requests.Load() != 0 {
				t.Fatalf("session requests = %d, want none before the guard", requests.Load())
			}
		})
	}
}

func noRetryHTTP() *retryablehttp.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 0
	client.CheckRetry = func(_ context.Context, _ *http.Response, err error) (bool, error) { return false, err }
	client.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errorsNewRedirect() }
	return client
}

func deleteTestClient(t *testing.T, url string, cert *x509.Certificate) *Client {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	client, err := NewClientWithGitHubApp(ClientWithGitHubAppConfig{
		GitHubConfigURL: url + "/owner/repo",
		GitHubAppAuth:   GitHubAppAuth{ClientID: "test-app", InstallationID: 42, PrivateKey: samplePrivateKey},
	}, WithRootCAs(roots), WithRetryableHTTPClint(noRetryHTTP()))
	if err != nil {
		t.Fatal(err)
	}
	return client
}
