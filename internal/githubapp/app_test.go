package githubapp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"
)

func fixture(t *testing.T, handler http.HandlerFunc) (*App, Installation) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	app, err := newApp("123", string(keyPEM), server.URL, server.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	installation := Installation{ID: 42, Selection: "selected", Permissions: map[string]string{"administration": "write"}}
	installation.Account.Login = "owner"
	return app, installation
}

func assertScaleSetTokenScope(t *testing.T, r *http.Request, repository string) {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		t.Fatalf("decode scale-set token scope: %v", err)
	}
	if len(raw) != 2 {
		t.Fatalf("unexpected scale-set token fields: %v", raw)
	}
	var repositories []string
	if err := json.Unmarshal(raw["repositories"], &repositories); err != nil || len(repositories) != 1 || repositories[0] != repository {
		t.Fatalf("unexpected repository scope: %q (%v)", repositories, err)
	}
	var permissions map[string]string
	if err := json.Unmarshal(raw["permissions"], &permissions); err != nil || len(permissions) != 1 || permissions["administration"] != "write" {
		t.Fatalf("unexpected permission scope: %v (%v)", permissions, err)
	}
}

func TestDiscoverAllPagesAndScope(t *testing.T) {
	var pages atomic.Int32
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("missing authentication")
		}
		switch r.URL.Path {
		case "/app/installations/42/access_tokens":
			if r.Method != "POST" {
				t.Error("wrong token method")
			}
			var body map[string]map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if len(body["permissions"]) != 1 || body["permissions"]["metadata"] != "read" {
				t.Error("discovery token grants excess permissions")
			}
			fmt.Fprint(w, `{"token":"discovery-only-secret"}`)
		case "/installation/repositories":
			pages.Add(1)
			if r.Header.Get("Authorization") != "Bearer discovery-only-secret" {
				t.Error("incorrect discovery auth")
			}
			if r.URL.Query().Get("per_page") != "100" {
				t.Error("missing pagination")
			}
			id := 1
			if r.URL.Query().Get("page") == "2" {
				id = 2
			}
			fmt.Fprintf(w, `{"total_count":2,"repositories":[{"id":%d,"full_name":"owner/repo%d","private":true}]}`, id, id)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	repos, err := app.Discover(context.Background(), installation)
	if err != nil || len(repos) != 2 || pages.Load() != 2 {
		t.Fatalf("repos=%v pages=%d err=%v", repos, pages.Load(), err)
	}
	if client, err := app.ScaleSetClient(installation, repos[0]); err != nil || client == nil {
		t.Fatalf("pinned SDK rejected its custom HTTP client: %v", err)
	}
}

func TestDiscoveryCannotReportFalseEmpty(t *testing.T) {
	for name, body := range map[string]string{
		"error-object":  `{"message":"not authorized"}`,
		"missing-array": `{"total_count":0}`,
		"missing-total": `{"repositories":[]}`,
		"incomplete":    `{"total_count":1,"repositories":[]}`,
		"foreign-owner": `{"total_count":1,"repositories":[{"id":1,"full_name":"another/repo"}]}`,
		"duplicate":     `{"total_count":2,"repositories":[{"id":1,"full_name":"owner/a"},{"id":1,"full_name":"owner/a"}]}`,
		"invalid-id":    `{"total_count":1,"repositories":[{"full_name":"owner/a"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					fmt.Fprint(w, `{"token":"secret"}`)
					return
				}
				fmt.Fprint(w, body)
			})
			if repos, err := app.Discover(context.Background(), installation); err == nil || repos != nil {
				t.Fatalf("accepted invalid discovery: %v %v", repos, err)
			}
		})
	}
}

func TestDiscoveryEmptyAndGrantChange(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					fmt.Fprint(w, `{"token":"secret"}`)
					return
				}
				if !change {
					fmt.Fprint(w, `{"total_count":0,"repositories":[]}`)
					return
				}
				if r.URL.Query().Get("page") == "1" {
					fmt.Fprint(w, `{"total_count":2,"repositories":[{"id":1,"full_name":"owner/a"}]}`)
					return
				}
				fmt.Fprint(w, `{"total_count":1,"repositories":[]}`)
			})
			_, err := app.Discover(context.Background(), installation)
			if (err != nil) != change {
				t.Fatalf("grant change %v returned %v", change, err)
			}
		})
	}
}

func TestInstallationSelectionAndBodyRedaction(t *testing.T) {
	for _, body := range []string{
		`{"id":42,"account":{"login":"owner"},"repository_selection":"selected","permissions":{"administration":"write"}}`,
		`{"id":42,"account":{"login":"foreign"},"repository_selection":"selected","permissions":{"administration":"write"}}`,
		// The operator keeps the App on "All repositories" (2026-09-21); what is
		// served is still decided by the allowlist and the private-only rule.
		`{"id":42,"account":{"login":"owner"},"repository_selection":"all","permissions":{"administration":"write"}}`,
		`{"id":42,"account":{"login":"owner"},"repository_selection":"selected","permissions":{"administration":"read"}}`,
		`{"id":42,"account":{"login":"owner"},"repository_selection":"all","permissions":{"administration":"read"}}`,
		`{"id":42,"account":{"login":"owner"},"repository_selection":"","permissions":{"administration":"write"}}`,
		`{"id":42,"account":{"login":"owner"},"repository_selection":"none","permissions":{"administration":"write"}}`,
	} {
		t.Run(body, func(t *testing.T) {
			app, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
			_, err := app.InstallationFor(context.Background(), "owner/repo")
			want := strings.Contains(body, `"foreign"`) || strings.Contains(body, `"read"`) ||
				strings.Contains(body, `"repository_selection":""`) || strings.Contains(body, `"repository_selection":"none"`)
			if (err != nil) != want {
				t.Fatalf("err=%v", err)
			}
		})
	}
	app, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-GitHub-Request-Id", "trace-123")
		w.WriteHeader(403)
		fmt.Fprint(w, `{"token":"must-never-leak"}`)
	})
	_, err := app.InstallationFor(context.Background(), "owner/repo")
	if err == nil || strings.Contains(err.Error(), "must-never-leak") {
		t.Fatalf("unsafe error: %v", err)
	}
	if app.LastHTTP().Status != 403 || app.LastHTTP().RequestID != "trace-123" {
		t.Fatalf("missing safe HTTP evidence: %+v", app.LastHTTP())
	}
}

func TestScaleSetClientRejectsInvalidInstallationScope(t *testing.T) {
	app, installation := fixture(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("invalid client construction reached GitHub")
	})
	repo := Repository{ID: 1, FullName: "owner/repo"}
	// Each case names the guard that must refuse it, so a case cannot pass
	// because some other guard happened to refuse first.
	const scopeRefusal = "requires selected or all-repositories installation"
	for name, tc := range map[string]struct {
		mutate  func(*Installation)
		refusal string
	}{
		"missing id":        {func(i *Installation) { i.ID = 0 }, scopeRefusal},
		"unknown selection": {func(i *Installation) { i.Selection = "none" }, scopeRefusal},
		"empty selection":   {func(i *Installation) { i.Selection = "" }, scopeRefusal},
		"read permission":   {func(i *Installation) { i.Permissions["administration"] = "read" }, scopeRefusal},
		"foreign owner":     {func(i *Installation) { i.Account.Login = "another" }, "repository outside installation owner"},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := installation
			candidate.Permissions = map[string]string{"administration": installation.Permissions["administration"]}
			tc.mutate(&candidate)
			client, err := app.ScaleSetClient(candidate, repo)
			if err == nil || client != nil {
				t.Fatalf("accepted invalid installation: client=%v err=%v", client, err)
			}
			if !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("refused by the wrong guard: want %q, got %v", tc.refusal, err)
			}
		})
	}
}

func TestScaleSetMutationIsNotRetriedOrRedirected(t *testing.T) {
	for _, status := range []int{503, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(status)
			})
			client := app.scaleSetHTTP(installation, Repository{ID: 1, FullName: "owner/repo"})
			client.Logger = nil
			resp, err := client.StandardClient().Post(app.baseURL+"/mutation", "application/json", strings.NewReader(`{}`))
			if resp != nil {
				resp.Body.Close()
			}
			if status == 503 && err == nil {
				t.Error("expected server failure")
			}
			if calls.Load() != 1 {
				t.Fatalf("mutation attempted %d times", calls.Load())
			}
		})
	}
}

func TestScaleSetTokenScopeFailsClosedOnUnexpectedInstallation(t *testing.T) {
	for _, path := range []string{
		"/api/v3/app/installations/99/access_tokens",
		"/api/v3/app/installations/42/access_tokens/",
		"/api/v3/installations/42/access_tokens",
		"/api/v3/app/installations/42/%61ccess_tokens",
	} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int32
			app, installation := fixture(t, func(http.ResponseWriter, *http.Request) {
				calls.Add(1)
			})
			client := app.scaleSetHTTP(installation, Repository{ID: 1, FullName: "owner/repo"})
			client.Logger = nil
			resp, err := client.StandardClient().Post(app.baseURL+path, "application/json", strings.NewReader(`{}`))
			if resp != nil {
				resp.Body.Close()
			}
			if err == nil {
				t.Fatal("unexpected installation-token target reached transport")
			}
			if calls.Load() != 0 {
				t.Fatalf("unexpected installation-token target reached server %d times", calls.Load())
			}
		})
	}
}

// Exercise the shipped SDK after admin authentication has overwritten its retry
// hooks. A helper-only HTTP test misses the second client's redirect policy.
func TestSDKMutationAfterAuthenticationIsNotReplayed(t *testing.T) {
	for _, status := range []int{200, 503, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var creates, redirects, adminCalls atomic.Int32
			var app *App
			var installation Installation
			app, installation = fixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v3/app/installations/42/access_tokens":
					assertScaleSetTokenScope(t, r, "repo")
					w.WriteHeader(201)
					fmt.Fprintf(w, `{"token":"test-installation-token","expires_at":%q}`, time.Now().Add(time.Hour).Format(time.RFC3339))
				case "/api/v3/repos/owner/repo/actions/runners/registration-token":
					w.WriteHeader(201)
					fmt.Fprint(w, `{"token":"test-registration-token"}`)
				case "/api/v3/actions/runner-registration":
					adminCalls.Add(1)
					token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString(app.key)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					fmt.Fprintf(w, `{"url":%q,"token":%q}`, app.baseURL+"/tenant", token)
				case "/tenant/_apis/runtime/runnerscalesets":
					if r.Method != "POST" {
						t.Errorf("unexpected method: %s", r.Method)
					}
					creates.Add(1)
					w.Header().Set("X-GitHub-Request-Id", "mutation-receipt")
					if status == 307 {
						w.Header().Set("Location", "/credential-leak")
					}
					w.WriteHeader(status)
					if status == 200 {
						fmt.Fprint(w, `{"id":17,"name":"probe","runnerGroupId":1}`)
					}
				case "/credential-leak":
					redirects.Add(1)
					w.WriteHeader(400)
				default:
					t.Errorf("unexpected path: %s", r.URL.Path)
					w.WriteHeader(404)
				}
			})
			client, err := scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
				GitHubConfigURL: app.baseURL + "/owner/repo",
				GitHubAppAuth:   scaleset.GitHubAppAuth{ClientID: app.id, InstallationID: installation.ID, PrivateKey: app.pem},
			}, scaleset.WithRetryableHTTPClint(app.scaleSetHTTP(installation, Repository{ID: 1, FullName: "owner/repo"})))
			if err != nil {
				t.Fatal(err)
			}
			set, err := client.CreateRunnerScaleSet(context.Background(), &scaleset.RunnerScaleSet{Name: "probe", RunnerGroupID: 1})
			if (err == nil) != (status == 200) {
				t.Fatalf("status=%d set=%+v error=%v", status, set, err)
			}
			if status == 200 && (set == nil || set.ID != 17) {
				t.Fatalf("positive control failed: %+v", set)
			}
			if adminCalls.Load() != 1 || creates.Load() != 1 || redirects.Load() != 0 {
				t.Fatalf("admin=%d mutations=%d redirects=%d", adminCalls.Load(), creates.Load(), redirects.Load())
			}
			if app.LastHTTP().Status != status || app.LastHTTP().RequestID != "mutation-receipt" {
				t.Fatalf("missing original mutation receipt: %+v", app.LastHTTP())
			}
		})
	}
}

// secretSentinels are recognisable stand-ins for every credential class that
// must stay inside the trusted process: App key, installation token, reusable
// registration token, JIT config, and any bearer header derived from them.
const (
	installationSentinel = "installation-token-sentinel"
	registrationSentinel = "registration-token-sentinel"
	jitSentinel          = "jit-config-sentinel"
)

// keyBody returns one long base64 line of the PEM. A whole-PEM match is easy to
// pass by accident; a single interior line is not.
func keyBody(t *testing.T, pem string) string {
	t.Helper()
	for _, line := range strings.Split(pem, "\n") {
		if len(line) >= 60 && !strings.Contains(line, "-----") {
			return line
		}
	}
	t.Fatal("no App key body line to scan for")
	return ""
}

// logSink collects everything the retryable HTTP client and the SDK choose to
// log, which is more than stdout/stderr capture sees: both loggers bind their
// writer when they are built.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logSink) bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.buf.Bytes()...)
}

type authRecorder struct {
	mu     sync.Mutex
	values []string
}

func (a *authRecorder) record(r *http.Request) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.values = append(a.values, strings.TrimPrefix(header, "Bearer "))
}

func (a *authRecorder) seen() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.values...)
}

// A redirect or a retry must never carry App, installation, or registration
// credentials to a host the caller did not authenticate to.
func TestCredentialsCannotReachAnotherHostByRedirectOrRetry(t *testing.T) {
	var foreignCalls atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignCalls.Add(1)
		t.Errorf("credential-bearing request reached another host: %s %s auth=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
	}))
	defer foreign.Close()
	tokenPath := "/api/v3/app/installations/42/access_tokens"

	t.Run("scale-set redirect", func(t *testing.T) {
		var calls atomic.Int32
		app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Location", foreign.URL+"/steal")
			w.WriteHeader(307)
		})
		client := app.scaleSetHTTP(installation, Repository{ID: 1, FullName: "owner/repo"})
		client.Logger = nil
		resp, err := client.StandardClient().Post(app.baseURL+tokenPath, "application/json", strings.NewReader(`{}`))
		if resp != nil {
			resp.Body.Close()
		}
		if err == nil {
			t.Fatal("cross-host redirect was followed")
		}
		if strings.Contains(err.Error(), installationSentinel) {
			t.Fatal("redirect error carried credential material")
		}
		if calls.Load() != 1 || foreignCalls.Load() != 0 {
			t.Fatalf("attempts=%d foreign=%d", calls.Load(), foreignCalls.Load())
		}
	})

	t.Run("scale-set retry", func(t *testing.T) {
		var calls atomic.Int32
		var bodies atomic.Int32
		app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if body, err := io.ReadAll(r.Body); err == nil && strings.Contains(string(body), "administration") {
				bodies.Add(1)
			}
			w.Header().Set("Location", foreign.URL+"/steal")
			w.WriteHeader(503)
		})
		client := app.scaleSetHTTP(installation, Repository{ID: 1, FullName: "owner/repo"})
		client.Logger = nil
		resp, err := client.StandardClient().Post(app.baseURL+tokenPath, "application/json", strings.NewReader(`{}`))
		if resp != nil {
			resp.Body.Close()
		}
		if err == nil {
			t.Fatal("exhausted retries reported success")
		}
		// The one-shot scoped body is sent exactly once and never to another target.
		if calls.Load() != 1 || bodies.Load() != 1 || foreignCalls.Load() != 0 {
			t.Fatalf("attempts=%d scoped-bodies=%d foreign=%d", calls.Load(), bodies.Load(), foreignCalls.Load())
		}
	})

	t.Run("discovery redirect", func(t *testing.T) {
		var calls atomic.Int32
		app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Location", foreign.URL+"/steal")
			w.WriteHeader(302)
		})
		if _, err := app.InstallationFor(context.Background(), "owner/repo"); err == nil {
			t.Fatal("discovery followed a cross-host redirect")
		}
		if _, err := app.Discover(context.Background(), installation); err == nil {
			t.Fatal("installation discovery followed a cross-host redirect")
		}
		// Both calls must have reached the fixture, or the refusals prove nothing.
		if calls.Load() < 2 || foreignCalls.Load() != 0 {
			t.Fatalf("attempts=%d foreign=%d", calls.Load(), foreignCalls.Load())
		}
	})
}

// Upstream bodies embed credentials. No error this package returns may repeat
// the App key, a signed JWT, or any token the API handed back.
func TestAppErrorsNeverCarrySecretMaterial(t *testing.T) {
	auth := &authRecorder{}
	var failing atomic.Bool
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		auth.record(r)
		if !failing.Load() {
			// One complete discovery, so the scale-set client below is reachable.
			if r.URL.Path == "/installation/repositories" {
				fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":1,"full_name":"owner/repo","private":true}]}`)
				return
			}
			fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
			return
		}
		w.Header().Set("X-GitHub-Request-Id", "receipt-1")
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"message":"upstream failure","token":%q,"registration_token":%q,"jit":%q,"authorization":%q}`,
			installationSentinel, registrationSentinel, jitSentinel, r.Header.Get("Authorization"))
	})
	repos, err := app.Discover(context.Background(), installation)
	if err != nil || len(repos) != 1 {
		t.Fatalf("discovery setup failed: %v", err)
	}
	client, err := app.ScaleSetClient(installation, repos[0])
	if err != nil || client == nil {
		t.Fatalf("scale-set client construction failed: %v", err)
	}
	failing.Store(true)
	var reported []string
	if _, err := app.InstallationFor(context.Background(), "owner/repo"); err == nil {
		t.Fatal("installation lookup reported success on HTTP 500")
	} else {
		reported = append(reported, err.Error())
	}
	if _, err := app.Discover(context.Background(), installation); err == nil {
		t.Fatal("discovery reported success on HTTP 500")
	} else {
		reported = append(reported, err.Error())
	}
	_, sdkErr := client.GenerateJitRunnerConfig(context.Background(), &scaleset.RunnerScaleSetJitRunnerSetting{Name: "probe", WorkFolder: "_work"}, 17)
	if sdkErr == nil {
		t.Fatal("SDK reported success on HTTP 500")
	}
	// SafeError is what the controller journals; the raw SDK error is not used.
	reported = append(reported, SafeError(sdkErr))
	receipt, err := json.Marshal(app.LastHTTP())
	if err != nil {
		t.Fatal(err)
	}
	reported = append(reported, string(receipt))
	secrets := append([]string{keyBody(t, app.pem), installationSentinel, registrationSentinel, jitSentinel}, auth.seen()...)
	if len(auth.seen()) == 0 {
		t.Fatal("no authenticated request was made; the scan proves nothing")
	}
	for _, text := range reported {
		for _, secret := range secrets {
			if secret != "" && strings.Contains(text, secret) {
				t.Fatalf("reported text carried credential material: %q", text)
			}
		}
	}
}

// Run the authenticated paths with both loggers captured and the App credential
// files on disk, then prove no credential class reached a log line or changed
// the credential directory. This package writes no journal or store of its own,
// so the on-disk half covers exactly the credential directory it reads; the
// controller's journal is scanned in internal/controller.
func TestNoSecretMaterialReachesLogsOrCredentialFiles(t *testing.T) {
	credentials := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	idFile, keyFile := filepath.Join(credentials, "app-id"), filepath.Join(credentials, "app.pem")
	if err := os.WriteFile(idFile, []byte("123"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte(keyPEM), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(idFile, keyFile); err != nil {
		t.Fatal(err)
	}
	auth := &authRecorder{}
	var app *App
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.record(r)
		switch r.URL.Path {
		case "/repos/owner/repo/installation":
			fmt.Fprint(w, `{"id":42,"account":{"login":"owner"},"repository_selection":"selected","permissions":{"administration":"write"}}`)
		case "/app/installations/42/access_tokens":
			fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
		case "/installation/repositories":
			fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":1,"full_name":"owner/repo","private":true}]}`)
		case "/api/v3/app/installations/42/access_tokens":
			assertScaleSetTokenScope(t, r, "repo")
			w.WriteHeader(201)
			fmt.Fprintf(w, `{"token":%q,"expires_at":%q}`, installationSentinel, time.Now().Add(time.Hour).Format(time.RFC3339))
		case "/api/v3/repos/owner/repo/actions/runners/registration-token":
			w.WriteHeader(201)
			fmt.Fprintf(w, `{"token":%q}`, registrationSentinel)
		case "/api/v3/actions/runner-registration":
			admin, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString(app.key)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			fmt.Fprintf(w, `{"url":%q,"token":%q}`, app.baseURL+"/tenant", admin)
		case "/tenant/_apis/runtime/runnerscalesets/17/generatejitconfig":
			fmt.Fprintf(w, `{"runner":{"id":77,"name":"probe","runnerScaleSetId":17},"encodedJITConfig":%q}`, jitSentinel)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	app, err = newApp("123", keyPEM, server.URL, server.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	// Both loggers are captured at their injection points. Reassigning
	// os.Stdout/os.Stderr would not capture the standard log package or anything
	// else that bound the file descriptor at init, so that half is not attempted.
	logs := &logSink{}
	run := func() error {
		installation, err := app.InstallationFor(context.Background(), "owner/repo")
		if err != nil {
			return err
		}
		repos, err := app.Discover(context.Background(), installation)
		if err != nil || len(repos) != 1 {
			return fmt.Errorf("discovery returned %d repositories: %w", len(repos), err)
		}
		if client, err := app.ScaleSetClient(installation, repos[0]); err != nil || client == nil {
			return fmt.Errorf("production scale-set client construction failed: %w", err)
		}
		// That client talks to github.com. Drive the identical HTTP path against
		// the fixture instead, so the production hooks still handle every request.
		// Both loggers are the verbose ones: production discards SDK output and
		// leaves retryablehttp's default logger in place, and neither may be
		// handed credential material to print.
		retries := app.scaleSetHTTP(installation, repos[0])
		retries.Logger = log.New(logs, "", 0)
		client, err := scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
			GitHubConfigURL: app.baseURL + "/owner/repo",
			GitHubAppAuth:   scaleset.GitHubAppAuth{ClientID: app.id, InstallationID: installation.ID, PrivateKey: app.pem},
		}, scaleset.WithRetryableHTTPClint(retries),
			scaleset.WithLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
		if err != nil {
			return err
		}
		config, err := client.GenerateJitRunnerConfig(context.Background(),
			&scaleset.RunnerScaleSetJitRunnerSetting{Name: "probe", WorkFolder: "_work"}, 17)
		if err != nil {
			return err
		}
		if config == nil || config.EncodedJITConfig != jitSentinel {
			return fmt.Errorf("JIT positive control failed: %+v", config)
		}
		return nil
	}
	if err := run(); err != nil {
		t.Fatalf("authenticated paths failed, so the scan proves nothing: %v", err)
	}
	logged := logs.bytes()
	if len(logged) == 0 {
		t.Fatal("no log line was produced; the scan cannot fail and proves nothing")
	}
	secrets := append([]string{keyBody(t, keyPEM), installationSentinel, registrationSentinel, jitSentinel}, auth.seen()...)
	for _, secret := range secrets {
		if secret != "" && bytes.Contains(logged, []byte(secret)) {
			t.Fatalf("credential material reached a log line: %q", secret)
		}
	}
	var files []string
	err = filepath.WalkDir(credentials, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		files = append(files, path)
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, secret := range secrets {
			// The App key file is the one place the key legitimately lives.
			if secret == "" || (path == keyFile && secret == keyBody(t, keyPEM)) {
				continue
			}
			if bytes.Contains(body, []byte(secret)) {
				t.Errorf("credential material was written to %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The two files this test wrote; anything else means the package wrote one.
	if len(files) != 2 {
		t.Fatalf("the credential directory changed during the run: %v", files)
	}
}

// The client's administrative token is minted for one repository, so any
// repository the installation did not grant must never reach the SDK. Owning
// the account is not being installed on the repository: only the last complete
// discovery says which repositories the installation granted.
func TestScaleSetClientRejectsRepositoryOutsideInstallation(t *testing.T) {
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/installation/repositories" {
			fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":1,"full_name":"owner/repo","private":true}]}`)
			return
		}
		fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
	})
	granted, err := app.Discover(context.Background(), installation)
	if err != nil || len(granted) != 1 {
		t.Fatalf("discovery setup failed: %v", err)
	}
	for name, repo := range map[string]Repository{
		"missing id":                {ID: 0, FullName: "owner/repo"},
		"foreign owner":             {ID: 1, FullName: "another/repo"},
		"bare name":                 {ID: 1, FullName: "repo"},
		"empty name":                {ID: 1, FullName: "owner/"},
		"extra segment":             {ID: 1, FullName: "owner/repo/../another"},
		"absolute owner":            {ID: 1, FullName: "/owner/repo"},
		"same owner, not installed": {ID: 2, FullName: "owner/never-installed"},
		"granted name, wrong id":    {ID: 9, FullName: "owner/repo"},
		"granted id, wrong name":    {ID: 1, FullName: "owner/other"},
	} {
		t.Run(name, func(t *testing.T) {
			if client, err := app.ScaleSetClient(installation, repo); err == nil || client != nil {
				t.Fatalf("accepted repository outside the installation: %+v", repo)
			}
		})
	}
	if client, err := app.ScaleSetClient(installation, granted[0]); err != nil || client == nil {
		t.Fatalf("positive control failed: %v", err)
	}
}

// An all-repositories installation discovers public repositories too, so the
// discovery snapshot is not the pool's bound. This client is the one choke
// point that builds an Administration-write credential, so it re-applies the
// private-and-active rule itself rather than trusting a caller's filtering.
func TestScaleSetClientRefusesPublicArchivedOrDisabled(t *testing.T) {
	for _, tc := range []struct {
		name     string
		repo     Repository
		accepted bool
	}{
		{"public", Repository{ID: 1, FullName: "owner/public"}, false},
		{"archived", Repository{ID: 2, FullName: "owner/archived", Private: true, Archived: true}, false},
		{"disabled", Repository{ID: 3, FullName: "owner/disabled", Private: true, Disabled: true}, false},
		{"private active", Repository{ID: 4, FullName: "owner/private", Private: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/installation/repositories" {
					body, err := json.Marshal(map[string]any{"total_count": 1, "repositories": []Repository{tc.repo}})
					if err != nil {
						t.Fatal(err)
					}
					fmt.Fprint(w, string(body))
					return
				}
				fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
			})
			installation.Selection = "all"
			granted, err := app.Discover(context.Background(), installation)
			if err != nil || len(granted) != 1 || granted[0] != tc.repo {
				t.Fatalf("discovery setup failed: %+v %v", granted, err)
			}
			client, err := app.ScaleSetClient(installation, granted[0])
			if tc.accepted {
				if err != nil || client == nil {
					t.Fatalf("private active repository refused: %v", err)
				}
				return
			}
			if err == nil || client != nil {
				t.Fatalf("accepted an ineligible repository: %+v", tc.repo)
			}
			if !strings.Contains(err.Error(), "public, archived or disabled") {
				t.Fatalf("refused for an unrelated reason: %v", err)
			}
		})
	}
}

// homelab#918: the caller's copy of a repository (the journal's row, or one a
// caller built) can lag the App's discovery -- the App records its snapshot
// before the journal commit, and the JIT/session paths never read the journal's
// archived/disabled columns at all. The private-and-active rule is therefore
// read from the discovery record, never from the caller's flags, and a refusal
// happens before any request leaves the process.
func TestScaleSetClientReadsVisibilityFromDiscoveryNotCaller(t *testing.T) {
	callerCopy := func(id int64, name string) Repository {
		return Repository{ID: id, FullName: name, Private: true}
	}
	for _, tc := range []struct {
		name       string
		discovered Repository
	}{
		{"discovery says public", Repository{ID: 1, FullName: "owner/flipped"}},
		{"discovery says archived", Repository{ID: 2, FullName: "owner/archived", Private: true, Archived: true}},
		{"discovery says disabled", Repository{ID: 3, FullName: "owner/disabled", Private: true, Disabled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path == "/installation/repositories" {
					body, err := json.Marshal(map[string]any{"total_count": 1, "repositories": []Repository{tc.discovered}})
					if err != nil {
						t.Error(err)
					}
					fmt.Fprint(w, string(body))
					return
				}
				fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
			})
			installation.Selection = "all"
			if _, err := app.Discover(context.Background(), installation); err != nil {
				t.Fatalf("discovery setup failed: %v", err)
			}
			before := requests.Load()
			client, err := app.ScaleSetClient(installation, callerCopy(tc.discovered.ID, tc.discovered.FullName))
			if err == nil || client != nil {
				t.Fatalf("built a client from the caller's private copy while discovery says %+v", tc.discovered)
			}
			if !strings.Contains(err.Error(), "public, archived or disabled") {
				t.Fatalf("refused for an unrelated reason: %v", err)
			}
			if after := requests.Load(); after != before {
				t.Fatalf("a refused client still made %d request(s)", after-before)
			}
		})
	}
}

// A repository that flips public between two complete discoveries is refused
// even to a caller still holding the copy the FIRST discovery returned.
func TestScaleSetClientRefusesStaleCopyAfterPublicFlip(t *testing.T) {
	var private atomic.Bool
	private.Store(true)
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/installation/repositories" {
			fmt.Fprintf(w, `{"total_count":1,"repositories":[{"id":1,"full_name":"owner/repo","private":%t}]}`, private.Load())
			return
		}
		fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
	})
	first, err := app.Discover(context.Background(), installation)
	if err != nil || len(first) != 1 || !first[0].Private {
		t.Fatalf("first discovery setup failed: %+v %v", first, err)
	}
	if client, err := app.ScaleSetClient(installation, first[0]); err != nil || client == nil {
		t.Fatalf("positive control failed while private: %v", err)
	}
	private.Store(false)
	if _, err := app.Discover(context.Background(), installation); err != nil {
		t.Fatalf("second discovery setup failed: %v", err)
	}
	if client, err := app.ScaleSetClient(installation, first[0]); err == nil || client != nil {
		t.Fatal("the stale private copy built a client for a repository discovery now reports public")
	}
}

// An App that has not completed a discovery grants no repository-scoped client.
func TestScaleSetClientRefusesBeforeAnyDiscovery(t *testing.T) {
	app, installation := fixture(t, func(http.ResponseWriter, *http.Request) {
		t.Fatal("client construction reached GitHub")
	})
	if client, err := app.ScaleSetClient(installation, Repository{ID: 1, FullName: "owner/repo"}); err == nil || client != nil {
		t.Fatal("a client was built without a discovery to authorize the repository")
	}
}

// homelab#921: RepositoryEligible is what a caller holding a cached client
// re-asks, so it must give ScaleSetClient's answer for every shape, including
// before any discovery. A drift between the two reopens the cached-client hole.
func TestRepositoryEligibleAgreesWithScaleSetClient(t *testing.T) {
	discovered := []Repository{
		{ID: 1, FullName: "owner/public"},
		{ID: 2, FullName: "owner/archived", Private: true, Archived: true},
		{ID: 3, FullName: "owner/disabled", Private: true, Disabled: true},
		{ID: 4, FullName: "owner/private", Private: true},
	}
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/installation/repositories" {
			body, err := json.Marshal(map[string]any{"total_count": len(discovered), "repositories": discovered})
			if err != nil {
				t.Error(err)
			}
			fmt.Fprint(w, string(body))
			return
		}
		fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
	})
	installation.Selection = "all"
	private := Repository{ID: 4, FullName: "owner/private", Private: true}
	if app.RepositoryEligible(private) {
		t.Fatal("eligible before any complete discovery")
	}
	if _, err := app.Discover(context.Background(), installation); err != nil {
		t.Fatalf("discovery setup failed: %v", err)
	}
	for _, tc := range []struct {
		name     string
		repo     Repository
		eligible bool
	}{
		{"public", discovered[0], false},
		{"archived", discovered[1], false},
		{"disabled", discovered[2], false},
		{"private active", private, true},
		{"caller copy says private, discovery says public", Repository{ID: 1, FullName: "owner/public", Private: true}, false},
		{"renamed", Repository{ID: 4, FullName: "owner/renamed", Private: true}, false},
		{"undiscovered", Repository{ID: 9, FullName: "owner/other", Private: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := app.RepositoryEligible(tc.repo)
			client, err := app.ScaleSetClient(installation, tc.repo)
			if got != tc.eligible {
				t.Fatalf("RepositoryEligible = %t, want %t", got, tc.eligible)
			}
			if built := err == nil && client != nil; built != got {
				t.Fatalf("RepositoryEligible = %t but ScaleSetClient built = %t (%v)", got, built, err)
			}
		})
	}
}

// A discovery that fails part way through is not a grant. Nothing it saw may
// authorize a client, including repositories from the pages that succeeded.
func TestPartialDiscoveryRecordsNoRepository(t *testing.T) {
	var pages atomic.Int32
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			fmt.Fprintf(w, `{"token":%q}`, installationSentinel)
			return
		}
		pages.Add(1)
		if r.URL.Query().Get("page") == "2" {
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, `{"total_count":2,"repositories":[{"id":1,"full_name":"owner/repo","private":true}]}`)
	})
	if repos, err := app.Discover(context.Background(), installation); err == nil || repos != nil {
		t.Fatalf("partial discovery reported success: %v %v", repos, err)
	}
	if pages.Load() != 2 {
		t.Fatalf("fixture served %d pages; the test did not reach the failure", pages.Load())
	}
	if client, err := app.ScaleSetClient(installation, Repository{ID: 1, FullName: "owner/repo"}); err == nil || client != nil {
		t.Fatal("a repository from a partial discovery authorized a client")
	}
}
