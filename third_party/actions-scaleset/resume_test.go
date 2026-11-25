package scaleset

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/actions/scaleset/internal/testserver"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
)

func TestResumeKnownIdentityWithoutOldQueueCredentials(t *testing.T) {
	for _, mode := range []string{
		"ok", "different uuid", "different owner", "different set", "different queue",
		"missing token", "missing stats", "null", "503", "401", "redirect",
		"truncated", "lost response", "invalid journal",
	} {
		t.Run(mode, func(t *testing.T) {
			var patches, creates, polls, deletes, redirects atomic.Int32
			identity := SessionResumeIdentity{ScaleSetID: 17, Owner: "sjo-linux-persisted-owner", SessionID: uuid.New()}
			var queue string
			server := testserver.NewUnstarted(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/sessions"):
					creates.Add(1)
					t.Error("resume POSTed a replacement session")
					w.WriteHeader(http.StatusInternalServerError)
				case r.URL.Path == "/redirected":
					redirects.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
				case r.Method == http.MethodPatch:
					patches.Add(1)
					if !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/17/sessions/%s", identity.SessionID)) {
						t.Error("wrong refresh scope")
					}
					body, err := io.ReadAll(r.Body)
					if err != nil || len(body) != 0 {
						t.Error("refresh requires no previous queue material")
					}
					if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || strings.Contains(r.Header.Get("Authorization"), "fresh-queue-token") {
						t.Error("refresh did not use administrative auth")
					}
					switch mode {
					case "503", "401":
						code := http.StatusServiceUnavailable
						if mode == "401" {
							code = http.StatusUnauthorized
						}
						w.WriteHeader(code)
						fmt.Fprint(w, "credential-error-sentinel")
						return
					case "redirect":
						w.Header().Set("Location", "/redirected")
						w.WriteHeader(http.StatusTemporaryRedirect)
						return
					case "truncated":
						w.Header().Set("Content-Length", "1000")
						fmt.Fprint(w, "{")
						return
					case "lost response":
						panic(http.ErrAbortHandler)
					case "null":
						fmt.Fprint(w, "null")
						return
					}
					session := RunnerScaleSetSession{
						SessionID: identity.SessionID, OwnerName: identity.Owner,
						RunnerScaleSet: &RunnerScaleSet{ID: 17}, MessageQueueURL: queue,
						MessageQueueAccessToken: "fresh-queue-token", Statistics: &RunnerScaleSetStatistic{},
					}
					switch mode {
					case "different uuid":
						session.SessionID = uuid.New()
					case "different owner":
						session.OwnerName = "foreign"
					case "different set":
						session.RunnerScaleSet.ID++
					case "different queue":
						session.MessageQueueURL += "/foreign"
					case "missing token":
						session.MessageQueueAccessToken = ""
					case "missing stats":
						session.Statistics = nil
					}
					json.NewEncoder(w).Encode(session)
				case r.Method == http.MethodGet && r.URL.Path == "/queue":
					polls.Add(1)
					if r.Header.Get("Authorization") != "Bearer fresh-queue-token" || r.URL.Query().Get("lastMessageId") != "42" || r.Header.Get(HeaderScaleSetMaxCapacity) != "0" {
						t.Error("resumed cursor, capacity, or token mismatch")
					}
					w.WriteHeader(http.StatusAccepted)
				case r.Method == http.MethodDelete:
					deletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			server.StartTLS()
			queue = server.URL + "/queue"
			identity.QueueSHA256 = sha256.Sum256([]byte(queue))
			if mode == "invalid journal" {
				identity.QueueSHA256 = [32]byte{}
			}
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			adminHTTP := retryablehttp.NewClient()
			adminHTTP.RetryMax = 0
			adminHTTP.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errorsNewRedirect() }
			client, err := NewClientWithGitHubApp(ClientWithGitHubAppConfig{
				GitHubConfigURL: server.URL + "/owner/repo",
				GitHubAppAuth:   GitHubAppAuth{ClientID: "test-app", InstallationID: 42, PrivateKey: samplePrivateKey},
			}, WithRootCAs(roots), WithRetryableHTTPClint(adminHTTP))
			if err != nil {
				t.Fatal(err)
			}
			sessionHTTP := retryablehttp.NewClient()
			sessionHTTP.RetryMax = 0
			sessionHTTP.CheckRetry = func(_ context.Context, _ *http.Response, err error) (bool, error) { return false, err }
			sessionHTTP.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errorsNewRedirect() }
			resumed, err := client.ResumeMessageSessionClient(context.Background(), identity, WithRetryableHTTPClint(sessionHTTP))
			// "missing stats" is the shape GitHub actually sends: its refresh (PATCH)
			// response carries no statistics, measured live on 2026-09-20. It was
			// listed here as a refusal, which pinned the assumption that fenced every
			// restarted controller.
			if mode == "ok" || mode == "missing stats" {
				if err != nil || resumed == nil {
					t.Fatalf("known session refresh failed: %v", err)
				}
				if resumed.commonClient.retryableHTTPClient != sessionHTTP || resumed.innerClient.commonClient.retryableHTTPClient != adminHTTP {
					t.Fatal("resume did not retain independently owned transport policy")
				}
				if msg, err := resumed.GetMessage(context.Background(), 42, 0); err != nil || msg != nil {
					t.Fatalf("resumed native poll failed: %v", err)
				}
				if err := resumed.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if polls.Load() != 1 || deletes.Load() != 1 {
					t.Fatal("native resumed handle not exercised")
				}
			} else {
				if err == nil || resumed != nil || strings.Contains(err.Error(), "credential-error-sentinel") {
					t.Fatalf("invalid recovery published a handle or leaked a body: %v", err)
				}
				if polls.Load() != 0 || deletes.Load() != 0 {
					t.Fatal("invalid recovery used queue credentials")
				}
			}
			wantPatches := int32(1)
			if mode == "invalid journal" {
				wantPatches = 0
			}
			if patches.Load() != wantPatches || creates.Load() != 0 || redirects.Load() != 0 {
				t.Fatal("resume retried, redirected, or replaced the session")
			}
		})
	}
}

func errorsNewRedirect() error { return fmt.Errorf("test refuses redirects") }
