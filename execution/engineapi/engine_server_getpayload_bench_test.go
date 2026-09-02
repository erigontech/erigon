// Copyright 2026 The Erigon Authors
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

package engineapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/jwt"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/rpc"
)

// BenchmarkEngineGetPayloadWithBlobs gives e2e measurements for engine_getPayload over a freshly
// built block carrying blobs (so the response includes the blobs bundle).
func BenchmarkEngineGetPayloadWithBlobs(b *testing.B) {
	if os.Getenv("ERIGON_RUN_GETPAYLOAD_BENCH") == "" {
		b.Skip("set ERIGON_RUN_GETPAYLOAD_BENCH=1 to run this full-node benchmark")
	}

	// A full mainnet block at bpo2 carries 21 blobs; raise the dev chain's per-block limit to match.
	const submitBlobs = 21

	logger := testlog.Logger(b, log.LvlError)
	ctx := context.Background()

	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(b, err)
	// genesis.Config is already a deep copy of chain.AllProtocolChanges; swap in a blob schedule that
	// allows 21 blobs/block across the late forks so the builder packs a full block.
	blobCfg := params.DefaultPragueBlobConfig
	blobCfg.Max = submitBlobs
	blobCfg.Target = 14
	schedule := make(map[string]*params.BlobConfig)
	for _, fork := range []string{"cancun", "prague", "osaka", "gloas", "bpo1", "bpo2", "bpo3", "bpo4", "bpo5"} {
		schedule[fork] = &blobCfg
	}
	genesis.Config.BlobSchedule = schedule

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     b.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, eat.Close()) })

	rpcClient, err := rpc.DialHTTP(eat.JsonRpcUrl, logger)
	require.NoError(b, err)
	b.Cleanup(rpcClient.Close)

	hashes := submitBlobTxns(ctx, b, eat, rpcClient, submitBlobs)

	// Wait until the blobs are in the pool so block building can pack them.
	require.Eventually(b, func() bool {
		resp, err := eat.EngineApiClient.GetBlobsV3(ctx, hashes)
		if err != nil || len(resp) != len(hashes) {
			return false
		}
		for _, bp := range resp {
			if bp == nil {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "blobs should become queryable before building")

	// Start building on top of the canonical head (packs the pending blob txns) and keep the payload
	// id so we can re-query engine_getPayload over the wire.
	payloadID, _, err := eat.MockCl.StartBuilding(ctx)
	require.NoError(b, err)

	// Fetch once to verify the built block actually carries blobs.
	built, err := eat.MockCl.GetBuiltPayload(ctx, payloadID)
	require.NoError(b, err)
	require.NotNil(b, built.BlobsBundle)
	require.NotEmpty(b, built.BlobsBundle.Blobs, "built block should carry blobs")

	method := "engine_getPayloadV4"
	if eat.ChainConfig.OsakaTime != nil {
		method = "engine_getPayloadV5"
	}
	if eat.ChainConfig.AmsterdamTime != nil {
		method = "engine_getPayloadV6"
	}
	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  []any{payloadID},
	})
	require.NoError(b, err)

	// Measure the server's response latency over real JSON-RPC WITHOUT the client-side decode: issue
	// a raw JWT-authenticated POST and drain the body to io.Discard.
	httpClient := &http.Client{Transport: jwt.NewHttpRoundTripper(http.DefaultTransport, eat.JwtSecret)}
	getPayloadRaw := func() (int64, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, eat.EngineApiUrl, bytes.NewReader(reqBody))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		httpResp, err := httpClient.Do(req)
		if err != nil {
			return 0, err
		}
		n, err := io.Copy(io.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
		if err != nil {
			return 0, err
		}
		if httpResp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("unexpected status %d", httpResp.StatusCode)
		}
		return n, nil
	}

	respBytes, err := getPayloadRaw()
	require.NoError(b, err)
	b.Logf("%s over JSON-RPC: %d blobs in bundle, ~%d KiB response (client decode excluded)", method, len(built.BlobsBundle.Blobs), respBytes/1024)

	b.SetBytes(respBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := getPayloadRaw(); err != nil {
			b.Fatal(err)
		}
	}
}
