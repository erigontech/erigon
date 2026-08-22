package sszql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const validQueryBody = `{"queries":[{"anchor":"execution_block","path":".transactions[0].to"}]}`

// Stands in for the JSON-RPC server, which answers on any path.
const fallbackStatus = http.StatusTeapot

const validHash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

// queryPattern mirrors the route registered in cmd/rpcdaemon/cli/config.go.
// The wildcard names must match what the handler reads via r.PathValue.
const (
	queryPattern              = "POST /eth/{version}/execution/{blockID}/query"
	queryTrailingSlashPattern = queryPattern + "/{$}"
)

func newTestMux() *http.ServeMux {
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(fallbackStatus)
	})
	mux := http.NewServeMux()
	mux.Handle("/", fallback)
	mux.Handle(queryPattern, SSZQueryHandler())
	mux.Handle(queryTrailingSlashPattern, SSZQueryHandler())
	return mux
}

func doRequest(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	return doRequestWithContentType(t, method, target, sszQLContentType, body)
}

func doRequestWithContentType(t *testing.T, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	newTestMux().ServeHTTP(rec, req)
	return rec
}

// assertJSONError checks the response is a JSON error document with the wanted code.
func assertJSONError(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Errorf("status: got %d, want %d (body %q)", rec.Code, want, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != sszQLContentType {
		t.Errorf("error Content-Type: got %q, want %q", ct, sszQLContentType)
	}
	var qe queryError
	if err := json.Unmarshal(rec.Body.Bytes(), &qe); err != nil {
		t.Fatalf("error body is not JSON: %v (body %q)", err, rec.Body.String())
	}
	if qe.Code != want {
		t.Errorf("error code field: got %d, want %d", qe.Code, want)
	}
	if qe.Message == "" {
		t.Error("error message is empty")
	}
}

func TestRouteMatchesQueryEndpoint(t *testing.T) {
	for _, path := range []string{
		"/eth/v1/execution/123/query",
		"/eth/v1/execution/0/query",
		"/eth/v1/execution/latest/query",
		"/eth/v1/execution/earliest/query",
		"/eth/v1/execution/safe/query",
		"/eth/v1/execution/finalized/query",
		"/eth/v1/execution/pending/query",
		"/eth/v1/execution/" + validHash + "/query",
		"/eth/v1/execution/123/query/",
	} {
		t.Run(path, func(t *testing.T) {
			rec := doRequest(t, http.MethodPost, path, validQueryBody)
			if rec.Code != http.StatusOK {
				t.Errorf("got status %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

func TestRouteFallsThroughToJSONRPC(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"root", http.MethodPost, "/"},
		{"two segments", http.MethodPost, "/eth/mainnet/query"},
		{"seven segments", http.MethodPost, "/eth/a/b/c/d/e/query"},
		{"too few segments", http.MethodPost, "/eth/v1/execution/query"},
		{"too many segments", http.MethodPost, "/eth/v1/execution/123/extra/query"},
		{"subtree below query", http.MethodPost, "/eth/v1/execution/123/query/extra"},
		{"wrong root", http.MethodPost, "/beacon/v1/execution/123/query"},
		{"wrong domain", http.MethodPost, "/eth/v1/consensus/123/query"},
		{"wrong suffix", http.MethodPost, "/eth/v1/execution/123/prove"},
		{"uppercase literal", http.MethodPost, "/eth/v1/EXECUTION/123/query"},
		{"health check on query path", http.MethodGet, "/eth/v1/execution/123/query"},
		{"health check on near-miss path", http.MethodGet, "/eth/foo/query"},
		{"put on query path", http.MethodPut, "/eth/v1/execution/123/query"},
		{"delete on query path", http.MethodDelete, "/eth/v1/execution/123/query"},
		{"patch on query path", http.MethodPatch, "/eth/v1/execution/123/query"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, tt.method, tt.path, validQueryBody)
			if rec.Code != fallbackStatus {
				t.Errorf("%s %q: got status %d, want fallback to JSON-RPC (%d)", tt.method, tt.path, rec.Code, fallbackStatus)
			}
		})
	}
}

// ServeMux cleans non-canonical paths and redirects rather than serving them, so
// no double-slash spelling can reach the handler by a different route than the
// canonical one.
func TestRouteRedirectsNonCanonicalPaths(t *testing.T) {
	tests := []struct{ path, wantLocation string }{
		{"//eth/v1/execution/123/query", "/eth/v1/execution/123/query"},
		{"/eth//v1/execution/123/query", "/eth/v1/execution/123/query"},
		{"/eth/v1/execution/123/query//", "/eth/v1/execution/123/query/"},
		{"/eth/v1/execution/../123/query", "/eth/v1/123/query"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			rec := doRequest(t, http.MethodPost, tt.path, validQueryBody)
			if rec.Code != http.StatusMovedPermanently {
				t.Errorf("got status %d, want %d", rec.Code, http.StatusMovedPermanently)
			}
			if got := rec.Header().Get("Location"); got != tt.wantLocation {
				t.Errorf("Location: got %q, want %q", got, tt.wantLocation)
			}
		})
	}
}

// Version segments strconv.Atoi would have accepted as aliases of a valid version,
// plus the out-of-range and malformed ones.
func TestRouteRejectsBogusVersion(t *testing.T) {
	for _, path := range []string{
		"/eth/v0/execution/123/query",
		"/eth/v2/execution/123/query",
		"/eth/v7/execution/123/query",
		"/eth/vx/execution/123/query",
		"/eth/v+1/execution/123/query",
		"/eth/v-1/execution/123/query",
		"/eth/v01/execution/123/query",
		"/eth/v0001/execution/123/query",
		"/eth/v256/execution/123/query",
		"/eth/1/execution/123/query",
	} {
		t.Run(path, func(t *testing.T) {
			assertJSONError(t, doRequest(t, http.MethodPost, path, validQueryBody), http.StatusNotFound)
		})
	}
}

func TestRouteRejectsBogusBlockID(t *testing.T) {
	for _, blockID := range []string{
		"head",           // beacon vocabulary, not ours
		"genesis",        // beacon vocabulary, not ours
		"latestExecuted", // Erigon-internal tag, not exposed
		"01",             // non-canonical decimal
		"+1",
		"-1",
		"0x7b",           // hex numbers are not part of the URL grammar
		"0x1234",         // too short for a hash
		validHash + "ab", // too long for a hash
		"LATEST",
	} {
		t.Run(blockID, func(t *testing.T) {
			path := "/eth/v1/execution/" + blockID + "/query"
			assertJSONError(t, doRequest(t, http.MethodPost, path, validQueryBody), http.StatusNotFound)
		})
	}
}

func TestRouteContentType(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		want        int
	}{
		{"missing", "", http.StatusUnsupportedMediaType},
		{"plain text", "text/plain", http.StatusUnsupportedMediaType},
		{"form encoded", "application/x-www-form-urlencoded", http.StatusUnsupportedMediaType},
		{"json", "application/json", http.StatusOK},
		{"json with charset", "application/json; charset=utf-8", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequestWithContentType(t, http.MethodPost, "/eth/v1/execution/123/query", tt.contentType, validQueryBody)
			if tt.want == http.StatusOK {
				if rec.Code != http.StatusOK {
					t.Errorf("got status %d, want %d", rec.Code, http.StatusOK)
				}
				return
			}
			assertJSONError(t, rec, tt.want)
		})
	}
}

func TestRouteInvalidJSON(t *testing.T) {
	for _, body := range []string{
		"",
		"not json",
		`{"queries":`,
		validQueryBody + `{"junk":1}`,
		validQueryBody + `[1,2]`,
		validQueryBody + `null`,
		validQueryBody + `garbage`,
		`null`,
		`{}`,
		`{"queries":[]}`,
		`{"queries":null}`,
		`[]`,
		`123`,
		`{"queries":[{"path":".a"}],"typo_field":1}`,
	} {
		t.Run(body, func(t *testing.T) {
			assertJSONError(t, doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body), http.StatusBadRequest)
		})
	}
}

// A body over the MaxBytesReader cap must be rejected rather than buffered.
func TestRouteRejectsOversizedBody(t *testing.T) {
	oversized := `{"queries":[{"anchor":"execution_block","path":"` + strings.Repeat("a", 1<<20) + `"}]}`
	assertJSONError(t, doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", oversized), http.StatusBadRequest)
}

func TestRouteAcceptsBodyUnderLimit(t *testing.T) {
	underLimit := `{"queries":[{"anchor":"execution_block","path":"` + strings.Repeat("a", 1024) + `"}]}`
	rec := doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", underLimit)
	if rec.Code != http.StatusOK {
		t.Errorf("got status %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// Trailing whitespace is not junk; a client appending a newline must still be served.
func TestRouteTrailingWhitespaceAccepted(t *testing.T) {
	for _, body := range []string{validQueryBody + "\n", validQueryBody + "   ", validQueryBody + " \t\n"} {
		rec := doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body)
		if rec.Code != http.StatusOK {
			t.Errorf("body %q: got status %d, want %d", body, rec.Code, http.StatusOK)
		}
	}
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) SSZQLResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != sszQLContentType {
		t.Errorf("Content-Type: got %q, want %q", ct, sszQLContentType)
	}
	var res SSZQLResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	return res
}

func TestRouteValidResponse(t *testing.T) {
	res := decodeResponse(t, doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", validQueryBody))

	if len(res.Paths) != 1 || res.Paths[0] != ".transactions[0].to" {
		t.Errorf("Paths: got %v, want [.transactions[0].to]", res.Paths)
	}
	if len(res.Results) != 1 {
		t.Errorf("Results: got %d entries, want 1", len(res.Results))
	}
	if len(res.Aliases) != 0 {
		t.Errorf("Aliases: got %d entries, want 0 when none requested", len(res.Aliases))
	}
	if len(res.Proofs) != 0 {
		t.Errorf("Proofs: got %d entries, want 0 when include_proofs is unset", len(res.Proofs))
	}
}

// The alias branch of parseQuery only runs when the request carries aliases.
func TestRouteAliasBranch(t *testing.T) {
	body := `{"aliases":[
		{"anchor":"execution_block","path":".stateRoot","alias":"root"},
		{"anchor":"execution_block","path":".number","alias":"num"}
	],"queries":[{"anchor":"execution_block","path":".transactions[0].to"}]}`

	res := decodeResponse(t, doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body))

	if len(res.Aliases) != 2 {
		t.Fatalf("Aliases: got %d entries, want 2", len(res.Aliases))
	}
	got := map[string]string{}
	for _, a := range res.Aliases {
		got[a.Alias] = a.Value
	}
	for _, name := range []string{"root", "num"} {
		if _, ok := got[name]; !ok {
			t.Errorf("alias %q missing from response: %v", name, got)
		}
	}
}

// The proof branch of parseQuery only runs when include_proofs is set.
func TestRouteProofBranch(t *testing.T) {
	body := `{"include_proofs":true,"queries":[
		{"anchor":"execution_block","path":".transactions[0].to"},
		{"anchor":"execution_block","path":".stateRoot"}
	]}`

	res := decodeResponse(t, doRequest(t, http.MethodPost, "/eth/v1/execution/123/query", body))

	if len(res.Proofs) != len(res.Results) {
		t.Errorf("Proofs: got %d entries, want one per result (%d)", len(res.Proofs), len(res.Results))
	}
	if len(res.Proofs) == 0 {
		t.Error("include_proofs was set but no proofs were returned")
	}
}

// Gindex marshals as a decimal string so values above 2^53 survive JSON.parse in
// browser clients.
func TestGindexMarshalsAsDecimalString(t *testing.T) {
	for _, g := range []Gindex{0, 1, 1 << 53, (1 << 53) + 1, 1152921504606846977, 1<<64 - 1} {
		b, err := json.Marshal(g)
		if err != nil {
			t.Fatalf("marshal %d: %v", uint64(g), err)
		}
		if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
			t.Errorf("Gindex(%d) marshalled unquoted as %s", uint64(g), b)
		}

		var round Gindex
		if err := json.Unmarshal(b, &round); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if round != g {
			t.Errorf("round trip: got %d, want %d", uint64(round), uint64(g))
		}
	}
}
