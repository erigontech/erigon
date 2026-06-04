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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	request := httptest.NewRequest(method, "http://url.com", strings.NewReader(body))
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

	request, err := http.NewRequest(method, ts.URL, strings.NewReader(body))
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

	resp, err := http.Post(ts.URL, "application/json", strings.NewReader(body))
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
