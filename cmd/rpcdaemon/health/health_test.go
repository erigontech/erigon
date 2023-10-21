package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/rpc"
)

type netApiStub struct {
	response hexutil.Uint
	error    error
}

func (n *netApiStub) PeerCount(_ context.Context) (hexutil.Uint, error) {
	return n.response, n.error
}

type ethApiStub struct {
	blockResult   map[string]interface{}
	blockError    error
	syncingResult interface{}
	syncingError  error
}

func (e *ethApiStub) GetBlockByNumber(_ context.Context, _ rpc.BlockNumber, _ bool) (map[string]interface{}, error) {
	return e.blockResult, e.blockError
}

func (e *ethApiStub) Syncing(_ context.Context) (interface{}, error) {
	return e.syncingResult, e.syncingError
}

func TestProcessHealthcheckIfNeeded_HeadersTests(t *testing.T) {
	cases := []struct {
		headers             []string
		netApiResponse      hexutil.Uint
		netApiError         error
		ethApiBlockResult   map[string]interface{}
		ethApiBlockError    error
		ethApiSyncingResult interface{}
		ethApiSyncingError  error
		expectedStatusCode  int
		expectedBody        map[string]string
	}{
		// 0 - sync check enabled - syncing
		{
			headers:             []string{"synced"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusOK,
			expectedBody: map[string]string{
				synced:           "HEALTHY",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 1 - sync check enabled - not syncing
		{
			headers:             []string{"synced"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: struct{}{},
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "ERROR: not synced",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 2 - sync check enabled - error checking sync
		{
			headers:             []string{"synced"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: struct{}{},
			ethApiSyncingError:  errors.New("problem checking sync"),
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "ERROR: problem checking sync",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 3 - peer count enabled - good request
		{
			headers:             []string{"min_peer_count1"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusOK,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "HEALTHY",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 4 - peer count enabled - not enough peers
		{
			headers:             []string{"min_peer_count10"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "ERROR: not enough peers: 1 (minimum 10)",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 5 - peer count enabled - error checking peers
		{
			headers:             []string{"min_peer_count10"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         errors.New("problem checking peers"),
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "ERROR: problem checking peers",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 6 - peer count enabled - badly formed request
		{
			headers:             []string{"min_peer_countABC"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   make(map[string]interface{}),
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "ERROR: strconv.Atoi: parsing \"abc\": invalid syntax",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 7 - block check - all ok
		{
			headers:             []string{"check_block10"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   map[string]interface{}{"test": struct{}{}},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusOK,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "HEALTHY",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 8 - block check - no block found
		{
			headers:             []string{"check_block10"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   map[string]interface{}{},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "ERROR: no known block with number 10 (a hex)",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 9 - block check - error checking block
		{
			headers:             []string{"check_block10"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   map[string]interface{}{},
			ethApiBlockError:    errors.New("problem checking block"),
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "ERROR: problem checking block",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 10 - block check - badly formed request
		{
			headers:             []string{"check_blockABC"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   map[string]interface{}{},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "ERROR: strconv.Atoi: parsing \"abc\": invalid syntax",
				maxSecondsBehind: "DISABLED",
			},
		},
		// 11 - seconds check - all ok
		{
			headers:        []string{"max_seconds_behind60"},
			netApiResponse: hexutil.Uint(1),
			netApiError:    nil,
			ethApiBlockResult: map[string]interface{}{
				"timestamp": time.Now().Add(1 * time.Second).Unix(),
			},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusOK,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "HEALTHY",
			},
		},
		// 12 - seconds check - too old
		{
			headers:        []string{"max_seconds_behind60"},
			netApiResponse: hexutil.Uint(1),
			netApiError:    nil,
			ethApiBlockResult: map[string]interface{}{
				"timestamp": uint64(time.Now().Add(1 * time.Hour).Unix()),
			},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "ERROR: timestamp too old: got ts:",
			},
		},
		// 13 - seconds check - less than 0 seconds
		{
			headers:        []string{"max_seconds_behind-1"},
			netApiResponse: hexutil.Uint(1),
			netApiError:    nil,
			ethApiBlockResult: map[string]interface{}{
				"timestamp": uint64(time.Now().Add(1 * time.Hour).Unix()),
			},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "ERROR: bad header value",
			},
		},
		// 14 - seconds check - badly formed request
		{
			headers:             []string{"max_seconds_behindABC"},
			netApiResponse:      hexutil.Uint(1),
			netApiError:         nil,
			ethApiBlockResult:   map[string]interface{}{},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusInternalServerError,
			expectedBody: map[string]string{
				synced:           "DISABLED",
				minPeerCount:     "DISABLED",
				checkBlock:       "DISABLED",
				maxSecondsBehind: "ERROR: strconv.Atoi: parsing \"abc\": invalid syntax",
			},
		},
		// 15 - all checks - report ok
		{
			headers:        []string{"synced", "check_block10", "min_peer_count1", "max_seconds_behind60"},
			netApiResponse: hexutil.Uint(10),
			netApiError:    nil,
			ethApiBlockResult: map[string]interface{}{
				"timestamp": time.Now().Add(1 * time.Second).Unix(),
			},
			ethApiBlockError:    nil,
			ethApiSyncingResult: false,
			ethApiSyncingError:  nil,
			expectedStatusCode:  http.StatusOK,
			expectedBody: map[string]string{
				synced:           "HEALTHY",
				minPeerCount:     "HEALTHY",
				checkBlock:       "HEALTHY",
				maxSecondsBehind: "HEALTHY",
			},
		},
	}

	for idx, c := range cases {
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "http://localhost:9090/health", nil)
		if err != nil {
			t.Errorf("%v: creating request: %v", idx, err)
		}

		for _, header := range c.headers {
			r.Header.Add("X-ERIGON-HEALTHCHECK", header)
		}

		netAPI := rpc.API{
			Namespace: "",
			Version:   "",
			Service: &netApiStub{
				response: c.netApiResponse,
				error:    c.netApiError,
			},
			Public: false,
		}

		ethAPI := rpc.API{
			Namespace: "",
			Version:   "",
			Service: &ethApiStub{
				blockResult:   c.ethApiBlockResult,
				blockError:    c.ethApiBlockError,
				syncingResult: c.ethApiSyncingResult,
				syncingError:  c.ethApiSyncingError,
			},
			Public: false,
		}

		apis := make([]rpc.API, 2)
		apis[0] = netAPI
		apis[1] = ethAPI

		ProcessHealthcheckIfNeeded(w, r, apis)

		result := w.Result()
		if result.StatusCode != c.expectedStatusCode {
			t.Errorf("%v: expected status code: %v, but got: %v", idx, c.expectedStatusCode, result.StatusCode)
		}

		bodyBytes, err := io.ReadAll(result.Body)
		if err != nil {
			t.Errorf("%v: reading response body: %s", idx, err)
		}

		var body map[string]string
		err = json.Unmarshal(bodyBytes, &body)
		if err != nil {
			t.Errorf("%v: unmarshalling the response body: %s", idx, err)
		}
		result.Body.Close()

		for k, v := range c.expectedBody {
			val, found := body[k]
			if !found {
				t.Errorf("%v: expected the key: %s to be in the response body but it wasn't there", idx, k)
			}
			if !strings.Contains(val, v) {
				t.Errorf("%v: expected the response body key: %s to contain: %s, but it contained: %s", idx, k, v, val)
			}
		}
	}
}

func TestProcessHealthcheckIfNeeded_RequestBody(t *testing.T) {
	cases := []struct {
		body               string
		netApiResponse     hexutil.Uint
		netApiError        error
		ethApiBlockResult  map[string]interface{}
		ethApiBlockError   error
		expectedStatusCode int
		expectedBody       map[string]string
	}{
		// 0 - happy path
		{
			body:               "{\"min_peer_count\": 1, \"known_block\": 123}",
			netApiResponse:     hexutil.Uint(1),
			netApiError:        nil,
			ethApiBlockResult:  map[string]interface{}{"test": struct{}{}},
			ethApiBlockError:   nil,
			expectedStatusCode: http.StatusOK,
			expectedBody: map[string]string{
				"healthcheck_query": "HEALTHY",
				"min_peer_count":    "HEALTHY",
				"check_block":       "HEALTHY",
			},
		},
		// 1 - bad request body
		{
			body:               "{\"min_peer_count\" 1, \"known_block\": 123}",
			netApiResponse:     hexutil.Uint(1),
			netApiError:        nil,
			ethApiBlockResult:  map[string]interface{}{"test": struct{}{}},
			ethApiBlockError:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody: map[string]string{
				"healthcheck_query": "ERROR:",
				"min_peer_count":    "DISABLED",
				"check_block":       "DISABLED",
			},
		},
		// 2 - min peers - error from api
		{
			body:               "{\"min_peer_count\": 1, \"known_block\": 123}",
			netApiResponse:     hexutil.Uint(1),
			netApiError:        errors.New("problem getting peers"),
			ethApiBlockResult:  map[string]interface{}{"test": struct{}{}},
			ethApiBlockError:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody: map[string]string{
				"healthcheck_query": "HEALTHY",
				"min_peer_count":    "ERROR: problem getting peers",
				"check_block":       "HEALTHY",
			},
		},
		// 3 - min peers - not enough peers
		{
			body:               "{\"min_peer_count\": 10, \"known_block\": 123}",
			netApiResponse:     hexutil.Uint(1),
			netApiError:        nil,
			ethApiBlockResult:  map[string]interface{}{"test": struct{}{}},
			ethApiBlockError:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody: map[string]string{
				"healthcheck_query": "HEALTHY",
				"min_peer_count":    "ERROR: not enough peers",
				"check_block":       "HEALTHY",
			},
		},
		// 4 - check block - no block
		{
			body:               "{\"min_peer_count\": 1, \"known_block\": 123}",
			netApiResponse:     hexutil.Uint(1),
			netApiError:        nil,
			ethApiBlockResult:  map[string]interface{}{},
			ethApiBlockError:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody: map[string]string{
				"healthcheck_query": "HEALTHY",
				"min_peer_count":    "HEALTHY",
				"check_block":       "ERROR: no known block with number ",
			},
		},
		// 5 - check block - error getting block info
		{
			body:               "{\"min_peer_count\": 1, \"known_block\": 123}",
			netApiResponse:     hexutil.Uint(1),
			netApiError:        nil,
			ethApiBlockResult:  map[string]interface{}{},
			ethApiBlockError:   errors.New("problem getting block"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody: map[string]string{
				"healthcheck_query": "HEALTHY",
				"min_peer_count":    "HEALTHY",
				"check_block":       "ERROR: problem getting block",
			},
		},
	}

	for idx, c := range cases {
		w := httptest.NewRecorder()
		r, err := http.NewRequest(http.MethodGet, "http://localhost:9090/health", nil)
		if err != nil {
			t.Errorf("%v: creating request: %v", idx, err)
		}

		r.Body = io.NopCloser(strings.NewReader(c.body))

		netAPI := rpc.API{
			Namespace: "",
			Version:   "",
			Service: &netApiStub{
				response: c.netApiResponse,
				error:    c.netApiError,
			},
			Public: false,
		}

		ethAPI := rpc.API{
			Namespace: "",
			Version:   "",
			Service: &ethApiStub{
				blockResult: c.ethApiBlockResult,
				blockError:  c.ethApiBlockError,
			},
			Public: false,
		}

		apis := make([]rpc.API, 2)
		apis[0] = netAPI
		apis[1] = ethAPI

		ProcessHealthcheckIfNeeded(w, r, apis)

		result := w.Result()
		if result.StatusCode != c.expectedStatusCode {
			t.Errorf("%v: expected status code: %v, but got: %v", idx, c.expectedStatusCode, result.StatusCode)
		}

		bodyBytes, err := io.ReadAll(result.Body)
		if err != nil {
			t.Errorf("%v: reading response body: %s", idx, err)
		}

		var body map[string]string
		err = json.Unmarshal(bodyBytes, &body)
		if err != nil {
			t.Errorf("%v: unmarshalling the response body: %s", idx, err)
		}
		result.Body.Close()

		for k, v := range c.expectedBody {
			val, found := body[k]
			if !found {
				t.Errorf("%v: expected the key: %s to be in the response body but it wasn't there", idx, k)
			}
			if !strings.Contains(val, v) {
				t.Errorf("%v: expected the response body key: %s to contain: %s, but it contained: %s", idx, k, v, val)
			}
		}
	}
}
