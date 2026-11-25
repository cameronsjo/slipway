package githubapp

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"
)

func TestNativeSDKCannotTurnMalformedListIntoAbsenceOrPanic(t *testing.T) {
	for _, test := range []struct {
		name, body string
		valid      bool
		count      int
	}{
		{"empty", `{"count":0,"value":[]}`, true, 0},
		{"one", `{"count":1,"value":[{"id":17,"name":"pool","runnerGroupId":1}]}`, true, 1},
		{"missing count", `{"value":[]}`, false, 0},
		{"missing value", `{"count":0}`, false, 0},
		{"null value", `{"count":0,"value":null}`, false, 0},
		{"null response", `null`, false, 0},
		{"panic shape", `{"count":1,"value":[]}`, false, 0},
		{"false absence", `{"count":0,"value":[{"id":17,"name":"pool","runnerGroupId":1}]}`, false, 0},
		{"negative count", `{"count":-1,"value":[]}`, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
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
					token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString(app.key)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					fmt.Fprintf(w, `{"url":%q,"token":%q}`, app.baseURL+"/tenant", token)
				case "/tenant/_apis/runtime/runnerscalesets", "/tenant/_apis/distributedtask/pools/0/agents":
					fmt.Fprint(w, test.body)
				default:
					t.Errorf("unexpected SDK request: %s", r.URL.Path)
					w.WriteHeader(404)
				}
			})
			client, err := scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{GitHubConfigURL: app.baseURL + "/owner/repo",
				GitHubAppAuth: scaleset.GitHubAppAuth{ClientID: app.id, InstallationID: installation.ID, PrivateKey: app.pem}}, scaleset.WithRetryableHTTPClint(app.scaleSetHTTP(installation, Repository{ID: 1, FullName: "owner/repo"})))
			if err != nil {
				t.Fatal(err)
			}
			sets, err := client.ListRunnerScaleSets(context.Background(), 1)
			if (err == nil) != test.valid || (test.valid && len(sets) != test.count) {
				t.Fatalf("native list result: %+v %v", sets, err)
			}
			set, err := client.GetRunnerScaleSet(context.Background(), 1, "pool")
			if (err == nil) != test.valid || (test.valid && (set != nil) != (test.count == 1)) {
				t.Fatalf("native lookup result: %+v %v", set, err)
			}
			runner, err := client.GetRunnerByName(context.Background(), "pool")
			if (err == nil) != test.valid || (test.valid && (runner != nil) != (test.count == 1)) {
				t.Fatalf("native runner lookup result: %+v %v", runner, err)
			}
		})
	}
}
