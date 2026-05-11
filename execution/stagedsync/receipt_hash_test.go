package stagedsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

// rpcHTTPClient has a bounded timeout so a stalled localhost:8545 cannot
// hang the test suite. The endpoint is best-effort (the test t.Skip()'s
// when RPC is unavailable), so a short timeout is appropriate.
var rpcHTTPClient = &http.Client{Timeout: 10 * time.Second}

// TestReceiptHashFromRPC fetches receipts from a running serial node
// and verifies that DeriveSha produces the correct receipt root.
// This test requires a running erigon node on localhost:8545.
func TestReceiptHashFromRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("requires running erigon node")
	}

	blockNum := uint64(24363971)
	blockHex := fmt.Sprintf("0x%x", blockNum)

	// Get header
	headerResp := rpcCall(t, "erigon_getHeaderByNumber", blockHex)
	var headerResult struct {
		ReceiptsRoot string `json:"receiptsRoot"`
	}
	require.NoError(t, json.Unmarshal(headerResp, &headerResult))
	expectedRoot := common.HexToHash(headerResult.ReceiptsRoot)
	t.Logf("Expected receipt root: %s", expectedRoot.Hex())

	// Get receipts
	receiptsResp := rpcCall(t, "eth_getBlockReceipts", blockHex)
	var rpcReceipts []json.RawMessage
	require.NoError(t, json.Unmarshal(receiptsResp, &rpcReceipts))
	t.Logf("Receipt count: %d", len(rpcReceipts))

	// Convert to types.Receipts
	var receipts types.Receipts
	for i, raw := range rpcReceipts {
		var r types.Receipt
		require.NoError(t, json.Unmarshal(raw, &r), "receipt %d", i)
		r.Bloom = types.CreateBloom(types.Receipts{&r})
		receipts = append(receipts, &r)
	}

	// Compute receipt hash
	computed := types.DeriveSha(receipts)
	t.Logf("Computed receipt root: %s", computed.Hex())
	require.Equal(t, expectedRoot, computed,
		"DeriveSha on serial node receipts must match header")

	// Now verify: if we strip blooms and recompute (like BlockPostValidation does),
	// does the hash still match?
	for _, r := range receipts {
		r.Bloom = types.Bloom{} // clear
	}
	for _, r := range receipts {
		r.Bloom = types.CreateBloom(types.Receipts{r})
	}
	recomputed := types.DeriveSha(receipts)
	t.Logf("Recomputed (bloom reset): %s", recomputed.Hex())
	require.Equal(t, expectedRoot, recomputed,
		"Bloom recomputation must not change receipt hash")

	// Check receipt status values
	for i, r := range receipts {
		if r.Status != 0 && r.Status != 1 {
			t.Errorf("TX %d: unexpected status %d", i, r.Status)
		}
		if i < 3 {
			t.Logf("TX %d: type=%d status=%d cumGas=%d gasUsed=%d logs=%d bloom=%x...",
				i, r.Type, r.Status, r.CumulativeGasUsed, r.GasUsed, len(r.Logs), r.Bloom[:8])
		}
	}
}

func rpcCall(t *testing.T, method string, params ...string) json.RawMessage {
	t.Helper()
	paramStr := ""
	if len(params) > 0 {
		quoted := make([]string, len(params))
		for i, p := range params {
			quoted[i] = fmt.Sprintf("%q", p)
		}
		paramStr = strings.Join(quoted, ",")
	}
	body := fmt.Sprintf(`{"jsonrpc":"2.0","method":"%s","params":[%s],"id":1}`, method, paramStr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:8545", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build RPC request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := rpcHTTPClient.Do(req)
	if err != nil {
		t.Skipf("RPC not available: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(data, &result))
	if result.Error != nil {
		t.Skipf("RPC error: %s", result.Error.Message)
	}
	return result.Result
}
