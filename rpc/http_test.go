// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc/jsonstream"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func confirmStatusCode(t *testing.T, got, want int) {
	t.Helper()
	if got == want {
		return
	}
	if gotName := http.StatusText(got); len(gotName) > 0 {
		if wantName := http.StatusText(want); len(wantName) > 0 {
			t.Fatalf("response status code: got %d (%s), want %d (%s)", got, gotName, want, wantName)
		}
	}
	t.Fatalf("response status code: got %d, want %d", got, want)
}

func confirmRequestValidationCode(t *testing.T, method, contentType, body string, expectedStatusCode int) {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), method, "http://url.com", strings.NewReader(body))
	if len(contentType) > 0 {
		request.Header.Set("Content-Type", contentType)
	}
	code, err := validateRequest(request)
	if code == 0 {
		if err != nil {
			t.Errorf("validation: got error %v, expected nil", err)
		}
	} else if err == nil {
		t.Errorf("validation: code %d: got nil, expected error", code)
	}
	confirmStatusCode(t, code, expectedStatusCode)
}

func TestHTTPErrorResponseWithDelete(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodDelete, contentType, "", http.StatusMethodNotAllowed)
}

func TestHTTPErrorResponseWithPut(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodPut, contentType, "", http.StatusMethodNotAllowed)
}

func TestHTTPErrorResponseWithMaxContentLength(t *testing.T) {
	body := make([]rune, maxRequestContentLength+1)
	confirmRequestValidationCode(t,
		http.MethodPost, contentType, string(body), http.StatusRequestEntityTooLarge)
}

func TestHTTPErrorResponseWithEmptyContentType(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodPost, "", "", http.StatusUnsupportedMediaType)
}

func TestHTTPErrorResponseWithValidRequest(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodPost, contentType, "", 0)
}

func confirmHTTPRequestYieldsStatusCode(t *testing.T, method, contentType, body string, expectedStatusCode int) {
	t.Helper()
	s := Server{}
	ts := httptest.NewServer(&s)
	defer ts.Close()

	request, err := http.NewRequestWithContext(t.Context(), method, ts.URL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create a valid HTTP request: %v", err)
	}
	if len(contentType) > 0 {
		request.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	confirmStatusCode(t, resp.StatusCode, expectedStatusCode)
}

func TestHTTPResponseWithEmptyGet(t *testing.T) {
	confirmHTTPRequestYieldsStatusCode(t, http.MethodGet, "", "", http.StatusOK)
}

// This checks that maxRequestContentLength is not applied to the response of a request.
func TestHTTPRespBodyUnlimited(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger := log.New()
	const respLength = maxRequestContentLength * 3

	s := NewServer(50, false /* traceRequests */, false /* debugSingleRequests */, true, logger, 100)
	defer s.Stop()
	if err := s.RegisterName("test", largeRespService{respLength}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var r string
	if err := c.Call(&r, "test_largeResp"); err != nil {
		t.Fatal(err)
	}
	if len(r) != respLength {
		t.Fatalf("response has wrong length %d, want %d", len(r), respLength)
	}
}

// TestHTTPBatchPreservesOrderWithStreaming checks that a batch mixing streamed (test_streamEcho)
// and non-streaming (test_echo) calls returns responses in request order — each answer at its index.
func TestHTTPBatchPreservesOrderWithStreaming(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body := `[` +
		`{"jsonrpc":"2.0","id":1,"method":"test_streamEcho","params":["one"]},` +
		`{"jsonrpc":"2.0","id":2,"method":"test_echo","params":["two",2,{"S":"x"}]},` +
		`{"jsonrpc":"2.0","id":3,"method":"test_streamEcho","params":["three"]},` +
		`{"jsonrpc":"2.0","id":4,"method":"test_echo","params":["four",4,{"S":"y"}]}` +
		`]`

	req, err := http.NewRequestWithContext(t.Context(), "POST", ts.URL, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &arr))
	require.Len(t, arr, 4)

	want := []struct {
		id     int
		result string
	}{
		{1, `"one"`},
		{2, `{"String":"two","Int":2,"Args":{"S":"x"}}`},
		{3, `"three"`},
		{4, `{"String":"four","Int":4,"Args":{"S":"y"}}`},
	}
	for i, w := range want {
		var m struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
		}
		require.NoError(t, json.Unmarshal(arr[i], &m))
		require.Equal(t, w.id, m.ID, "response at index %d is out of order", i)
		require.JSONEq(t, w.result, string(m.Result), "wrong result at index %d", i)
	}
}

func TestHTTPPeerInfo(t *testing.T) {
	logger := log.New()
	s := newTestServer(logger)
	defer s.Stop()
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := Dial(ts.URL, logger)
	if err != nil {
		t.Fatal(err)
	}
	c.SetHeader("user-agent", "ua-testing")
	c.SetHeader("origin", "origin.example.com")

	// Request peer information.
	var info PeerInfo
	if err := c.Call(&info, "test_peerInfo"); err != nil {
		t.Fatal(err)
	}

	if info.RemoteAddr == "" {
		t.Error("RemoteAddr not set")
	}
	if info.Transport != "http" {
		t.Errorf("wrong Transport %q", info.Transport)
	}
	if info.HTTP.Version != "HTTP/1.1" {
		t.Errorf("wrong HTTP.Version %q", info.HTTP.Version)
	}
	if info.HTTP.UserAgent != "ua-testing" {
		t.Errorf("wrong HTTP.UserAgent %q", info.HTTP.UserAgent)
	}
	if info.HTTP.Origin != "origin.example.com" {
		t.Errorf("wrong HTTP.Origin %q", info.HTTP.UserAgent)
	}
}

func signJwt(t *testing.T, secret []byte) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()})
	signed, err := token.SignedString(secret)
	require.NoError(t, err)
	return signed
}

// Auth-scheme names are case-insensitive (RFC 7235 §2.1).
func TestCheckJwtSecretAuthScheme(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	token := signJwt(t, secret)

	cases := []struct {
		name   string
		header string
		want   bool
	}{
		{"canonical scheme", "Bearer " + token, true},
		{"lowercase scheme", "bearer " + token, true},
		{"uppercase scheme", "BEARER " + token, true},
		{"mixed case scheme", "BeArEr " + token, true},
		{"empty header", "", false},
		{"scheme without token", "Bearer ", false},
		{"header shorter than scheme", "Bear", false},
		{"other scheme", "Basic " + token, false},
		{"foreign secret", "Bearer " + signJwt(t, []byte("fedcba9876543210fedcba9876543210")), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://url.com", nil)
			r.Header.Set("Authorization", tc.header)
			require.Equal(t, tc.want, CheckJwtSecret(httptest.NewRecorder(), r, secret))
		})
	}
}

// overloadService stands in for a method whose DB gate rejected the request,
// once per callback shape: a plain method answers through writeTo, a streamable
// one writes its own envelope before the rejection happens.
type overloadService struct{}

func (*overloadService) Reject(context.Context) (string, error) { return "", kv.ErrReadTxLimitExceeded }

func (*overloadService) RejectStreaming(_ context.Context, _ jsonstream.Stream) error {
	return kv.ErrReadTxLimitExceeded
}

// TestOverloadedRequestGets503 pins that a single request rejected by the DB gate
// answers 503, not 200, on the streaming path. The JSON-RPC error body is written
// before ServeHTTP can set the status, so anything that puts those bytes on the
// wire early makes net/http commit 200 and discard the real status. Batch requests
// and disabled streaming answer 200 through plumbing this test does not reach.
func TestOverloadedRequestGets503(t *testing.T) {
	for _, method := range []string{"test_reject", "test_rejectStreaming"} {
		t.Run(method, func(t *testing.T) {
			srv := NewServer(50, false, false, false /* disableStreaming */, log.Root(), 100)
			defer srv.Stop()
			require.NoError(t, srv.RegisterName("test", new(overloadService)))

			body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":[]}`
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)

			require.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
			require.Contains(t, rec.Body.String(), ErrMsgServerOverloaded)
		})
	}
}

// hangUpWriter accepts headers, then fails every body write, standing in for a
// client that goes away mid-response.
type hangUpWriter struct {
	header http.Header
	status int
}

func (w *hangUpWriter) Header() http.Header { return w.header }
func (w *hangUpWriter) WriteHeader(s int)   { w.status = s }
func (w *hangUpWriter) Write([]byte) (int, error) {
	return 0, errors.New("connection reset by peer")
}

// TestUndeliveredResponseIsCounted pins that a reply the client never received
// is recorded. The status is already sent by then, so the counter is the only
// place a truncated reply can show up.
func TestUndeliveredResponseIsCounted(t *testing.T) {
	srv := NewServer(50, false, false, false /* disableStreaming */, log.Root(), 100)
	defer srv.Stop()
	require.NoError(t, srv.RegisterName("test", new(testService)))

	before := undeliveredGauge.GetValueUint64()

	body := `{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",1]}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(&hangUpWriter{header: make(http.Header)}, req)

	require.Greater(t, undeliveredGauge.GetValueUint64(), before,
		"a response the client never received was not recorded")
}

// TestHTTPRequestFraming covers what the server answers for the shapes of body
// that reach it, including the ones that are not valid JSON.
func TestHTTPRequestFraming(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string // substring the response must contain, empty means no response
	}{
		{"call", `{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",3]}`, `"result"`},
		{"call with surrounding space", "  \n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test_echo\",\"params\":[\"x\",3]}\n ", `"result"`},
		{"batch", `[{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",3]}]`, `"result"`},
		{"empty body", ``, ``},
		{"whitespace only", "   \n\t ", ``},
		{"truncated object", `{"jsonrpc":"2.0","id":1,"method":"test_echo"`, `parse error`},
		{"not json", `hello`, `parse error`},
		// JSON whitespace is space, tab, CR and LF only. Unicode whitespace is a body.
		{"vertical tab body", "\v", `parse error`},
		{"form feed body", "\f", `parse error`},
		{"no-break space body", "\u00a0", `parse error`},
		{"unbalanced bracket", `[{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",3]}`, `parse error`},
		{"control character in string", "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test_\x01echo\",\"params\":[]}", `parse error`},
		// A body holding more than one value is rejected. The decoder this
		// replaced stopped after the first value and ignored the rest.
		{"trailing second value", `{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",3]}{"a":1}`, `parse error`},
		{"trailing garbage", `{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",3]} oops`, `parse error`},
		// Valid JSON that is not an object reaches the handler as a zero message
		// and is an invalid request, not a parse error.
		{"bare number body", `1`, `"code":-32600`},
		{"bare string body", `"str"`, `"code":-32600`},
		{"bare bool body", `true`, `"code":-32600`},
		// Only the offending field is dropped, so the id still comes back and the
		// caller can match the error to its request.
		{"wrong-type field keeps the id", `{"jsonrpc":"2.0","id":7,"method":123,"params":["a",1]}`, `"id":7`},
	}

	srv := newTestServer(log.New())
	defer srv.Stop()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(tc.body))
			req.Header.Set("content-type", "application/json")
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)

			body := rec.Body.String()
			if tc.want == "" {
				require.Empty(t, strings.TrimSpace(body), "want no response")
				return
			}
			require.Contains(t, body, tc.want)
		})
	}
}

// TestHTTPRequestFramingChunked checks a body with no content length, which is
// the case the size hint cannot help with.
func TestHTTPRequestFramingChunked(t *testing.T) {
	srv := newTestServer(log.New())
	defer srv.Stop()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body := `{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["` + strings.Repeat("x", 40000) + `",3]}`
	req, err := http.NewRequestWithContext(t.Context(), "POST", ts.URL, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = -1 // forces chunked transfer encoding
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(raw), strings.Repeat("x", 40000))
}

// TestHTTPBatchRequestFraming checks a batch whose items each carry a sizeable
// argument. Every message in a batch points into the same buffer, so this would
// catch one item's arguments bleeding into another's.
func TestHTTPBatchRequestFraming(t *testing.T) {
	srv := newTestServer(log.New())
	defer srv.Stop()

	var items []string
	for i := range 8 {
		arg := strings.Repeat(string(rune('a'+i)), 5000)
		items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"test_echo","params":[%q,%d]}`, i+1, arg, i))
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader("["+strings.Join(items, ",")+"]"))
	req.Header.Set("content-type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &arr))
	require.Len(t, arr, 8)
	for i := range arr {
		var m struct {
			ID     int `json:"id"`
			Result struct {
				String string `json:"String"`
				Int    int    `json:"Int"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(arr[i], &m))
		require.Equal(t, i+1, m.ID, "response %d is out of order", i)
		require.Equal(t, strings.Repeat(string(rune('a'+i)), 5000), m.Result.String, "argument %d bled", i)
		require.Equal(t, i, m.Result.Int, "argument %d bled", i)
	}
}

// TestReadAllBody checks the body reader against io.ReadAll for both a helpful
// and an unhelpful size hint.
func TestReadAllBody(t *testing.T) {
	for _, size := range []int{0, 1, 511, 512, 513, 40000} {
		want := make([]byte, size)
		for i := range want {
			want[i] = byte(i)
		}
		// an oversized hint is what a lying Content-Length produces
		for _, hint := range []int{0, size, size * 2, 1, int(maxBodySizeHint)} {
			got, err := readAllBody(bytes.NewReader(want), hint)
			require.NoError(t, err)
			require.Equal(t, want, got, "size %d hint %d", size, hint)
		}
	}
}

// TestReadAllBodyError checks that a read failure is reported rather than
// treated as the end of the body.
func TestReadAllBodyError(t *testing.T) {
	_, err := readAllBody(&errReader{}, 0)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

type errReader struct{}

func (*errReader) Read([]byte) (int, error) { return 0, io.ErrClosedPipe }
