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
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/jwt"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// BenchmarkEngineGetBlobsV3WorstCasePayload measures engine_getBlobsV3 over the real JSON-RPC
// transport for the largest payload mainnet currently permits, isolating the JSON serialization +
// transport cost at the engine API surface (issue #21226) from the txpool lookup itself.
func BenchmarkEngineGetBlobsV3WorstCasePayload(b *testing.B) {
	if os.Getenv("ERIGON_RUN_GETBLOBS_BENCH") == "" {
		b.Skip("set ERIGON_RUN_GETBLOBS_BENCH=1 to run this full-node benchmark")
	}

	// mainnet's worst-case blobs per block (the bpo2 max), each with its full set of cell proofs.
	const maxBlobs = 21

	logger := testlog.Logger(b, log.LvlError)
	ctx := context.Background()

	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, b.TempDir())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, eat.Close()) })

	rpcClient, err := rpc.DialHTTP(eat.JsonRpcUrl, logger)
	require.NoError(b, err)
	b.Cleanup(rpcClient.Close)

	hashes := submitBlobTxns(ctx, b, eat, rpcClient, maxBlobs)

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
	}, 30*time.Second, 100*time.Millisecond, "all %d blobs should become queryable", maxBlobs)

	// Measure the server's response latency over real JSON-RPC WITHOUT the client-side decode:
	// issue a raw JWT-authenticated POST and drain the body to io.Discard. The client never
	// hex-decodes the multi-MB payload into structs — in production that cost is the consensus
	// layer's, not erigon's — so the timing reflects what the node is actually charged for.
	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "engine_getBlobsV3",
		"params":  []any{hashes},
	})
	require.NoError(b, err)

	httpClient := &http.Client{Transport: jwt.NewHttpRoundTripper(http.DefaultTransport, eat.JwtSecret)}
	getBlobsRaw := func() (int64, error) {
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

	respBytes, err := getBlobsRaw()
	require.NoError(b, err)
	require.Greater(b, respBytes, int64(1<<20), "response must carry the blobs")
	b.Logf("worst-case getBlobsV3 over JSON-RPC: %d blobs, %d cell proofs/blob, ~%d KiB response (client decode excluded)", maxBlobs, params.CellsPerExtBlob, respBytes/1024)

	b.SetBytes(respBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := getBlobsRaw()
		if err != nil {
			b.Fatal(err)
		}
		if n < 1<<20 {
			b.Fatalf("short response: %d bytes", n)
		}
	}
}

// submitBlobTxns submits maxBlobs distinct blobs to the node, split across the fewest wrapped blob
// txns allowed by MaxBlobsPerTxn, all signed by the funded coinbase. It returns the blob versioned
// hashes in submission order.
func submitBlobTxns(ctx context.Context, b *testing.B, eat engineapitester.EngineApiTester, rpcClient *rpc.Client, maxBlobs int) []common.Hash {
	b.Helper()
	coinbaseAddr := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
	nonceBig, err := eat.RpcApiClient.GetTransactionCount(coinbaseAddr, rpc.PendingBlock)
	require.NoError(b, err)
	nonce := nonceBig.Uint64()

	hashes := make([]common.Hash, 0, maxBlobs)
	tag := byte(0)
	for remaining := maxBlobs; remaining > 0; {
		count := min(remaining, params.MaxBlobsPerTxn)
		tags := make([]byte, count)
		for i := range tags {
			tag++
			tags[i] = tag
		}
		wrapper := buildWrappedBlobTxn(b, eat.ChainId(), eat.CoinbaseKey, nonce, tags)
		var buf bytes.Buffer
		require.NoError(b, wrapper.MarshalBinaryWrapped(&buf))
		var txnHash common.Hash
		require.NoError(b, rpcClient.CallContext(ctx, &txnHash, "eth_sendRawTransaction", hexutil.Bytes(buf.Bytes())))
		hashes = append(hashes, wrapper.Tx.BlobVersionedHashes...)
		nonce++
		remaining -= count
	}
	require.Len(b, hashes, maxBlobs)
	return hashes
}

// buildWrappedBlobTxn builds a Fulu-era (cell-proof) wrapped blob txn carrying one distinct blob per
// tag, signed by key. Each blob is zero apart from one low-order byte set to its tag, which keeps
// every field element a valid BLS scalar while giving every blob a distinct commitment.
func buildWrappedBlobTxn(b *testing.B, chainID *uint256.Int, key *ecdsa.PrivateKey, nonce uint64, tags []byte) *types.BlobTxWrapper {
	b.Helper()
	to := common.Address{0x10}
	wrapper := &types.BlobTxWrapper{WrapperVersion: 1}
	wrapper.Tx.To = &to
	wrapper.Tx.Nonce = nonce
	wrapper.Tx.GasLimit = 500_000
	wrapper.Tx.ChainID = *chainID
	wrapper.Tx.TipCap = *uint256.NewInt(1_000_000_000)
	wrapper.Tx.FeeCap = *uint256.NewInt(1_000_000_000_000)
	wrapper.Tx.MaxFeePerBlobGas = *uint256.NewInt(1_000_000_000)

	wrapper.Blobs = make(types.Blobs, len(tags))
	wrapper.Commitments = make(types.BlobKzgs, len(tags))
	wrapper.Proofs = make(types.KZGProofs, 0, len(tags)*int(params.CellsPerExtBlob))
	wrapper.Tx.BlobVersionedHashes = make([]common.Hash, len(tags))

	kzgCtx := kzg.Ctx()
	for i, tag := range tags {
		wrapper.Blobs[i][len(wrapper.Blobs[i])-1] = tag
		commitment, err := kzgCtx.BlobToKZGCommitment((*goethkzg.Blob)(&wrapper.Blobs[i]), 0)
		require.NoError(b, err)
		copy(wrapper.Commitments[i][:], commitment[:])
		_, cellProofs, err := kzgCtx.ComputeCellsAndKZGProofs((*goethkzg.Blob)(&wrapper.Blobs[i]), 4)
		require.NoError(b, err)
		for _, p := range &cellProofs {
			wrapper.Proofs = append(wrapper.Proofs, types.KZGProof(p))
		}
		wrapper.Tx.BlobVersionedHashes[i] = common.Hash(kzg.KZGToVersionedHash(commitment))
	}

	signed, err := types.SignTx(wrapper, *types.LatestSignerForChainID(chainID), key)
	require.NoError(b, err)
	v, r, s := signed.RawSignatureValues()
	wrapper.Tx.V.Set(v)
	wrapper.Tx.R.Set(r)
	wrapper.Tx.S.Set(s)
	return wrapper
}
