package da

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"github.com/stretchr/testify/require"
)

func TestClient_GetOffChainData(t *testing.T) {
	tests := []struct {
		name       string
		hash       common.Hash
		result     string
		data       []byte
		statusCode int
		err        string
	}{
		{
			name:   "successfully got offhcain data",
			hash:   common.BytesToHash([]byte("hash")),
			result: fmt.Sprintf(`{"result":"0x%s"}`, hex.EncodeToString([]byte("offchaindata"))),
			data:   []byte("offchaindata"),
		},
		{
			name:   "error returned by server",
			hash:   common.BytesToHash([]byte("hash")),
			result: `{"error":{"code":123,"message":"test error"}}`,
			err:    "123 test error",
		},
		{
			name:   "invalid offchain data returned by server",
			hash:   common.BytesToHash([]byte("hash")),
			result: `{"result":"invalid-signature"}`,
			err:    "hex string without 0x prefix",
		},
		{
			name:       "unsuccessful status code returned by server",
			hash:       common.BytesToHash([]byte("hash")),
			statusCode: http.StatusUnauthorized,
			err:        "invalid status code, expected: 200, found: 401",
		},
		{
			name:       "handle retry on 429",
			hash:       common.BytesToHash([]byte("hash")),
			statusCode: http.StatusTooManyRequests,
			err:        "max attempts of data fetching reached",
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var res types.Request
				require.NoError(t, json.NewDecoder(r.Body).Decode(&res))
				require.Equal(t, "sync_getOffChainData", res.Method)

				var params []common.Hash
				require.NoError(t, json.Unmarshal(res.Params, &params))
				require.Equal(t, tt.hash, params[0])

				if tt.statusCode > 0 {
					w.WriteHeader(tt.statusCode)
				}

				_, err := fmt.Fprint(w, tt.result)
				require.NoError(t, err)
			}))
			defer svr.Close()

			got, err := GetOffChainData(context.Background(), svr.URL, tt.hash)
			if tt.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.data, got)
			}
		})
	}
}
