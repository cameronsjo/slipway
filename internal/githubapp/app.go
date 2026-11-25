// Package githubapp discovers the selected or all-repositories installation
// without exporting its credentials to the caller, a worker, the journal, or
// diagnostics.
package githubapp

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"
	"github.com/hashicorp/go-retryablehttp"
)

var repositoryName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9_.-]+$`)

type Repository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
	Private  bool   `json:"private"`
	Archived bool   `json:"archived"`
	Disabled bool   `json:"disabled"`
}

type Installation struct {
	ID      int64 `json:"id"`
	Account struct {
		Login string `json:"login"`
	} `json:"account"`
	Selection   string            `json:"repository_selection"`
	Permissions map[string]string `json:"permissions"`
}

// HTTPFailure deliberately excludes response bodies, query strings and headers
// other than the request ID. Upstream errors can include credential-bearing bodies.
type HTTPFailure struct {
	Method    string `json:"method"`
	Host      string `json:"host"`
	Path      string `json:"path"`
	Status    int    `json:"status"`
	RequestID string `json:"request_id,omitempty"`
}

type recorder struct {
	mu   sync.Mutex
	last HTTPFailure
	base http.RoundTripper
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.base.RoundTrip(req)
	entry := HTTPFailure{Method: req.Method, Host: req.URL.Host, Path: req.URL.EscapedPath()}
	if resp != nil {
		entry.Status = resp.StatusCode
		entry.RequestID = resp.Header.Get("X-GitHub-Request-Id")
	}
	r.mu.Lock()
	r.last = entry
	r.mu.Unlock()
	return resp, err
}

type App struct {
	id      string
	key     *rsa.PrivateKey
	pem     string
	baseURL string
	http    *http.Client
	trace   *recorder
	// discovered holds the last complete discovery, repository ID to the
	// repository as GitHub reported it (name and visibility). An App that has
	// not discovered yet grants no repository-scoped credential.
	discoveryMu sync.Mutex
	discovered  map[int64]Repository
	// log is nil in production, which means slog.Default() at call time;
	// tests set it to capture what a mint logged.
	log *slog.Logger
}

func (a *App) logger() *slog.Logger {
	if a.log != nil {
		return a.log
	}
	return slog.Default()
}

func New(idFile, keyFile string) (*App, error) {
	id, err := os.ReadFile(idFile)
	if err != nil {
		return nil, errors.New("cannot read App ID file")
	}
	pem, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, errors.New("cannot read App key file")
	}
	return newApp(strings.TrimSpace(string(id)), strings.ReplaceAll(string(pem), `\n`, "\n"), "https://api.github.com", http.DefaultTransport)
}

func newApp(id, pem, baseURL string, transport http.RoundTripper) (*App, error) {
	if id == "" {
		return nil, errors.New("empty App ID")
	}
	base, err := url.Parse(baseURL)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("invalid GitHub API base URL")
	}
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(pem))
	if err != nil {
		return nil, errors.New("invalid App RSA key")
	}
	r := &recorder{base: transport}
	return &App{id: id, key: key, pem: pem, baseURL: baseURL, trace: r,
		http: &http.Client{Transport: r, Timeout: 30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (a *App) LastHTTP() HTTPFailure {
	a.trace.mu.Lock()
	defer a.trace.mu.Unlock()
	return a.trace.last
}

func (a *App) jwt() (string, error) {
	now := time.Now()
	return jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Issuer: a.id, IssuedAt: jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(9 * time.Minute)),
	}).SignedString(a.key)
}

func (a *App) request(ctx context.Context, method, path, token string, body any, target any) error {
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("invalid API request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return errors.New("GitHub API transport failed; inspect HTTP receipt")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("GitHub API HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(target); err != nil {
		return errors.New("invalid GitHub API JSON")
	}
	return nil
}

// InstallationFor requires an explicitly selected bootstrap repository. The App
// may have other installations; discovery must never silently select one.
func (a *App) InstallationFor(ctx context.Context, repo string) (Installation, error) {
	var installation Installation
	if !repositoryName.MatchString(repo) {
		return installation, errors.New("bootstrap repo must be owner/repository")
	}
	token, err := a.jwt()
	if err != nil {
		return installation, errors.New("JWT signing failed")
	}
	err = a.request(ctx, "GET", "/repos/"+repo+"/installation", token, nil, &installation)
	if err != nil {
		return installation, err
	}
	if installation.ID <= 0 || !strings.EqualFold(installation.Account.Login, strings.Split(repo, "/")[0]) ||
		(installation.Selection != "selected" && installation.Selection != "all") || installation.Permissions["administration"] != "write" {
		return Installation{}, errors.New("expected selected or all-repositories owner installation with Administration write")
	}
	return installation, nil
}

func (a *App) Discover(ctx context.Context, installation Installation) ([]Repository, error) {
	access, err := a.discoveryToken(ctx, installation)
	if err != nil {
		return nil, err
	}
	return a.discoverWith(ctx, installation, access.Token)
}

// discoveryPermissions is the one permission set a discovery token is minted
// for: installation-wide metadata visibility, not Administration.
var discoveryPermissions = map[string]string{"metadata": "read"}

// discoveryTokenResponse is the mint response a discovery token arrives in.
// Discover reads only Token; the queue janitor's discovery also checks
// Permissions. Permissions stays raw so that decoding it can never fail a
// pool discovery that did not look at it before.
type discoveryTokenResponse struct {
	Token       string          `json:"token"`
	Permissions json.RawMessage `json:"permissions"`
}

// discoveryToken mints the metadata:read installation token discovery lists
// repositories with. It neither verifies the grant nor revokes the token:
// callers decide both.
func (a *App) discoveryToken(ctx context.Context, installation Installation) (discoveryTokenResponse, error) {
	var access discoveryTokenResponse
	token, err := a.jwt()
	if err != nil {
		return access, errors.New("JWT signing failed")
	}
	err = a.request(ctx, "POST", fmt.Sprintf("/app/installations/%d/access_tokens", installation.ID), token,
		map[string]any{"permissions": discoveryPermissions}, &access)
	if err != nil {
		return discoveryTokenResponse{}, err
	}
	if access.Token == "" {
		return discoveryTokenResponse{}, errors.New("installation access token missing")
	}
	return access, nil
}

// discoverWith lists every repository the installation grants, using an
// already minted discovery token, and records the result as the last complete
// discovery.
func (a *App) discoverWith(ctx context.Context, installation Installation, token string) ([]Repository, error) {
	var repositories []Repository
	seen := map[int64]bool{}
	expected := -1
	for page := 1; page <= 1000; page++ {
		var response struct {
			Total        *int          `json:"total_count"`
			Repositories *[]Repository `json:"repositories"`
		}
		if err := a.request(ctx, "GET", fmt.Sprintf("/installation/repositories?per_page=100&page=%d", page), token, nil, &response); err != nil {
			return nil, err
		}
		if response.Total == nil || response.Repositories == nil || *response.Total < 0 {
			return nil, errors.New("invalid repository-list shape")
		}
		// Under an all-repositories installation, unrelated account activity (a
		// repository created or deleted mid-pagination) changes total_count and
		// fails this one discovery cycle; the next cycle self-heals.
		if expected != -1 && expected != *response.Total {
			return nil, errors.New("repository grant changed during discovery; retry complete discovery")
		}
		expected = *response.Total
		for _, repo := range *response.Repositories {
			if repo.ID <= 0 || seen[repo.ID] || !repositoryName.MatchString(repo.FullName) || !strings.EqualFold(strings.Split(repo.FullName, "/")[0], installation.Account.Login) {
				return nil, errors.New("invalid, duplicate, or foreign repository in discovery")
			}
			seen[repo.ID] = true
			repositories = append(repositories, repo)
		}
		if len(repositories) == expected {
			a.recordDiscovery(repositories)
			return repositories, nil
		}
		if len(*response.Repositories) == 0 || len(repositories) > expected {
			return nil, errors.New("incomplete repository discovery")
		}
	}
	return nil, errors.New("repository discovery pagination limit exceeded")
}

func (a *App) recordDiscovery(repositories []Repository) {
	snapshot := make(map[int64]Repository, len(repositories))
	for _, repo := range repositories {
		snapshot[repo.ID] = repo
	}
	a.discoveryMu.Lock()
	defer a.discoveryMu.Unlock()
	a.discovered = snapshot
}

// discoveredRecord returns the repository exactly as the last complete
// discovery reported it, so a caller's own copy of the visibility flags is
// never what authorizes a credential. ok is false when the ID and full name do
// not both match a discovered repository.
func (a *App) discoveredRecord(repo Repository) (Repository, bool) {
	a.discoveryMu.Lock()
	defer a.discoveryMu.Unlock()
	record, found := a.discovered[repo.ID]
	return record, found && repo.FullName != "" && record.FullName == repo.FullName
}

var (
	errNotDiscovered = errors.New("repository absent from the last complete discovery")
	errNotEligible   = errors.New("repository is public, archived or disabled")
)

// discoveredEligibility is the pool's one repository rule: the last complete
// discovery holds this exact ID and full name, and records it private, not
// archived and not disabled. Every credential path reads it from here, so the
// build-time check and the cached-client check cannot drift apart.
func (a *App) discoveredEligibility(repo Repository) error {
	record, ok := a.discoveredRecord(repo)
	if !ok {
		return errNotDiscovered
	}
	if !record.Private || record.Archived || record.Disabled {
		return errNotEligible
	}
	return nil
}

// RepositoryEligible reports whether the App's last complete discovery still
// admits repo under the rule ScaleSetClient applies. A caller holding a client
// built earlier re-asks this before every use: the answer can change with any
// discovery, and nothing else tells a cached client that it has (homelab#921).
// It is false when no complete discovery has been recorded.
func (a *App) RepositoryEligible(repo Repository) bool {
	return a.discoveredEligibility(repo) == nil
}

// hasDiscovery reports whether any complete discovery has been recorded yet.
func (a *App) hasDiscovery() bool {
	a.discoveryMu.Lock()
	defer a.discoveryMu.Unlock()
	return a.discovered != nil
}

func (a *App) ScaleSetClient(installation Installation, repo Repository) (*scaleset.Client, error) {
	if installation.ID <= 0 || (installation.Selection != "selected" && installation.Selection != "all") || installation.Permissions["administration"] != "write" {
		return nil, errors.New("scale-set client requires selected or all-repositories installation with Administration write")
	}
	if repo.ID <= 0 || !repositoryName.MatchString(repo.FullName) || !strings.EqualFold(strings.Split(repo.FullName, "/")[0], installation.Account.Login) {
		return nil, errors.New("repository outside installation owner")
	}
	// Owning the account is not being installed on the repository. Only the last
	// complete discovery says which repositories this installation granted, and
	// every production caller discovers before it builds a client. Discovery is
	// not the pool's bound, though: an all-repositories installation discovers
	// every repository in the account, public ones included.
	//
	// The pool's private-only rule is re-applied here because this is the one
	// choke point where an Administration-write client is built. No caller's
	// own filtering is trusted to stand in for it, and neither are the caller's
	// flags: they come from the journal, which can lag the discovery this App
	// recorded (homelab#918). The flags are read from the discovery record.
	switch err := a.discoveredEligibility(repo); {
	case errors.Is(err, errNotDiscovered):
		return nil, errors.New("repository absent from the installation's last complete discovery")
	case err != nil:
		return nil, errors.New("scale-set client refuses a public, archived or disabled repository")
	}
	// The upstream client keeps installation and registration credentials in the
	// trusted process. No credential is emitted by this API. The supplied HTTP
	// client narrows the native SDK's token request to this exact repository.
	return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: "https://github.com/" + repo.FullName,
		GitHubAppAuth:   scaleset.GitHubAppAuth{ClientID: a.id, InstallationID: installation.ID, PrivateKey: a.pem},
		SystemInfo:      scaleset.SystemInfo{System: "slipway", Subsystem: "foundation-probe"},
	}, scaleset.WithRetryableHTTPClint(a.scaleSetHTTP(installation, repo)), scaleset.WithTimeout(30*time.Second),
		scaleset.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
}

func (a *App) scaleSetHTTP(installation Installation, repo Repository) *retryablehttp.Client {
	// WithRetryMax(0) alone becomes FOUR retries in the pinned client's defaults.
	// Pass an explicit client so a lost mutation response is never blindly replayed.
	retries := retryablehttp.NewClient()
	retries.RetryMax = 0
	retries.HTTPClient.Timeout = 30 * time.Second
	// The pinned SDK otherwise requests an installation-wide token with an empty
	// body. Its retry hook is the last point before the native request reaches the
	// transport, and the exact SDK pin/hash prevents this path from drifting
	// silently. Scope every such request to this client's one repository and only
	// the Administration permission required by runner registration management.
	repository := strings.Split(repo.FullName, "/")[1]
	scope, _ := json.Marshal(map[string]any{
		"repositories": []string{repository},
		"permissions":  map[string]string{"administration": "write"},
	})
	base, err := url.Parse(a.baseURL)
	if err != nil {
		panic("githubapp: validated API base URL became invalid")
	}
	tokenPath := fmt.Sprintf("/app/installations/%d/access_tokens", installation.ID)
	retries.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, _ int) {
		if req == nil || req.URL == nil || req.Method != http.MethodPost {
			return
		}
		// The SDK's installation-token endpoint is the only POST whose path
		// contains both terms. Treat every such request as token-looking so a
		// path drift (including a trailing slash or escaped spelling) cannot
		// fall through with the SDK's original unscoped empty body.
		path := req.URL.Path
		lowerPath := strings.ToLower(path)
		if !strings.Contains(lowerPath, "installation") || !strings.Contains(lowerPath, "token") {
			return
		}
		if req.URL.RawPath != "" || req.URL.Scheme != base.Scheme || req.URL.Host != base.Host ||
			(path != tokenPath && path != "/api/v3"+tokenPath) {
			// RequestLogHook cannot return an error. Make any unexpected token target
			// untransportable so SDK drift cannot mint a broader credential.
			req.URL.Scheme = "runner-token-scope-refused"
			req.URL.Host = ""
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(scope))
		req.ContentLength = int64(len(scope))
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(scope)), nil }
		req.Header.Set("Content-Type", "application/json")
	}
	// The SDK overwrites CheckRetry during admin authentication. Refuse at the
	// inner HTTP client's redirect boundary with an ordinary error instead:
	// ErrUseLastResponse would let StandardClient's outer wrapper follow it.
	retries.HTTPClient.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		if req.Response != nil {
			a.recordScaleSetResponse(req.Response)
		}
		return errors.New("GitHub API redirect refused")
	}
	// The pinned SDK insists Transport is *http.Transport. Use its supported
	// response hook, not our discovery RoundTripper, to record safe diagnostics.
	retries.ResponseLogHook = func(_ retryablehttp.Logger, resp *http.Response) {
		a.recordScaleSetResponse(resp)
		guardRunnerResponse(resp)
	}
	return retries
}

type rejectedScaleSetBody struct{}

func (rejectedScaleSetBody) Read([]byte) (int, error) {
	return 0, errors.New("invalid runner API response")
}
func (rejectedScaleSetBody) Close() error { return nil }

// Both native name lookups trust Count before indexing Value. Validate lists
// before SDK decoding so malformed JSON cannot become panic or false absence.
// Bound JIT responses too; the caller still validates registration identity.
func guardRunnerResponse(resp *http.Response) {
	path := resp.Request.URL.Path
	listResponse := resp.Request.Method == http.MethodGet && (strings.HasSuffix(path, "/_apis/runtime/runnerscalesets") ||
		strings.HasSuffix(path, "/_apis/distributedtask/pools/0/agents"))
	jitResponse := resp.Request.Method == http.MethodPost && strings.Contains(path, "/_apis/runtime/runnerscalesets/") && strings.HasSuffix(path, "/generatejitconfig")
	if resp.StatusCode != http.StatusOK || (!listResponse && !jitResponse) {
		return
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	closeErr := resp.Body.Close()
	var list struct {
		Count *int               `json:"count"`
		Value *[]json.RawMessage `json:"value"`
	}
	if readErr != nil || closeErr != nil || len(body) > 8<<20 ||
		(listResponse && (json.Unmarshal(bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf}), &list) != nil ||
			list.Count == nil || list.Value == nil || *list.Count != len(*list.Value))) {
		resp.Body = rejectedScaleSetBody{}
		return
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
}

func (a *App) recordScaleSetResponse(resp *http.Response) {
	a.trace.mu.Lock()
	defer a.trace.mu.Unlock()
	a.trace.last = HTTPFailure{Method: resp.Request.Method, Host: resp.Request.URL.Host,
		Path: resp.Request.URL.EscapedPath(), Status: resp.StatusCode, RequestID: resp.Header.Get("X-GitHub-Request-Id")}
}

// SafeError is used instead of printing upstream errors (which embed response
// bodies). Exact HTTP status and request IDs are recorded separately when available.
func SafeError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, scaleset.UnauthorizedError):
		return "unauthorized"
	case errors.Is(err, scaleset.NotFoundError):
		return "not-found"
	case errors.Is(err, scaleset.BadRequestError):
		return "bad-request"
	case errors.Is(err, scaleset.ConflictError):
		return "conflict"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return "API failure; outcome requires reconciliation"
	}
}
