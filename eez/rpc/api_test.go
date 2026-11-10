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

package rpc_test

import (
	"context"
	"encoding/json"
	"maps"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	eezrpc "github.com/erigontech/erigon/eez/rpc"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
	erigonrpc "github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// TestRender is T-NET-9 on the field set: a 0x76 transaction renders the
// fields of the reference client's SystemTransaction serializer and the
// location fields every transaction has.
func TestRender(t *testing.T) {
	eez.Register()
	tx, err := systemtx.New(protocol.SystemTx{ChainID: 6290, Nonce: 2, To: protocol.EEZL2Address, Value: *uint256.NewInt(3), Input: []byte{1}})
	require.NoError(t, err)
	b, err := json.Marshal(ethapi.NewRPCTransaction(tx, common.Hash{1}, 10, 5, 0, uint256.NewInt(7)))
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(b, &fields))

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	require.Equal(t, []string{
		"blockHash", "blockNumber", "blockTimestamp", "chainId", "from", "gas", "gasPrice", "hash", "input",
		"nonce", "r", "s", "to", "transactionIndex", "type", "v", "value",
	}, keys)
	require.Equal(t, "0x76", fields["type"])
	require.Equal(t, "0x1e8480", fields["gas"])
	require.Equal(t, "0x0", fields["gasPrice"])
	require.Equal(t, "0x1892", fields["chainId"])
	require.Equal(t, protocol.SystemAddress, common.HexToAddress(fields["from"].(string)))
}

func renderedKeys(t *testing.T, tx types.Transaction) (map[string]any, []string) {
	t.Helper()
	b, err := json.Marshal(ethapi.NewRPCTransaction(tx, common.Hash{1}, 10, 5, 0, uint256.NewInt(7)))
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(b, &fields))
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return fields, keys
}

// TestRenderV01 checks the field sets of types 0x45 and 0x46: those of the
// rollupX v0.1 serializers but sourceHash, blobVersionedHashes and
// authorizationList, and version and blocks.
func TestRenderV01(t *testing.T) {
	eez.Register()
	native, err := systemtx.NewNative(protocol.NativeTx{ChainID: 699169, GasLimit: 1_000, To: protocol.RollupXEEZL2Address, Input: []byte{1}})
	require.NoError(t, err)
	fields, keys := renderedKeys(t, native)
	require.Equal(t, []string{
		"accessList", "blockHash", "blockNumber", "blockTimestamp", "chainId", "from", "gas", "gasPrice", "hash", "input",
		"nonce", "r", "s", "to", "transactionIndex", "type", "v", "value", "yParity",
	}, keys)
	require.Equal(t, "0x45", fields["type"])
	require.Equal(t, "0x3e8", fields["gas"])
	require.Equal(t, "0x0", fields["gasPrice"])
	require.Equal(t, "0x0", fields["yParity"])
	require.Equal(t, protocol.NativeSystemAddress, common.HexToAddress(fields["from"].(string)))

	history, err := systemtx.NewHistory(protocol.HistoryTx{ChainID: 699169, Blocks: []protocol.HistoryBlock{{Number: 1, Hash: common.Hash{1}}}})
	require.NoError(t, err)
	fields, keys = renderedKeys(t, history)
	require.Equal(t, []string{
		"blockHash", "blockNumber", "blockTimestamp", "chainId", "from", "gas", "gasPrice", "hash", "input",
		"nonce", "r", "s", "to", "transactionIndex", "type", "v", "value", "yParity",
	}, keys)
	require.Equal(t, "0x46", fields["type"])
	require.Equal(t, "0x1e8480", fields["gas"])
	require.Equal(t, "0x", fields["input"])
	require.Equal(t, protocol.HistoryAddress, common.HexToAddress(fields["to"].(string)))
}

// TestDecodeBatch decodes the window-84 batch and the span it carries.
func TestDecodeBatch(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := eezrpc.DecodeBatch(w.PostBatch)
	require.NoError(t, err)
	require.Equal(t, []hexutil.Uint64{1}, b.RollupIDs)
	require.Empty(t, b.PayloadError)
	require.Equal(t, hexutil.Uint64(1), b.Payload.RollupID)
	require.Len(t, b.Payload.Blocks, len(w.Blocks))
	post, err := w.OracleHash("window_post_block_hash")
	require.NoError(t, err)
	last := b.Entries[len(b.Entries)-1].RollupUpdates
	require.Equal(t, post, last[len(last)-1].NewRoot)

	_, err = eezrpc.DecodeBatch([]byte{1, 2, 3, 4})
	require.ErrorIs(t, err, protocol.ErrSelector)
}

// TestAPI serves the eez namespace over JSON-RPC for a booted follower.
func TestAPI(t *testing.T) {
	c := derivertest.NewComposer(t)
	b := c.Compose(nil, derivertest.Block{Coinbase: common.Address{1}, Extra: []byte{}, Txs: [][]byte{c.Transfer(common.Address{2}, 1)}})
	l1Block := c.Post(derivertest.Posting{Batch: b}).Header.Number

	api := eezrpc.NewAPI()
	server := erigonrpc.NewServer(1, false, false, true, log.New(), 0)
	reg := api.API()
	require.NoError(t, server.RegisterName(reg.Namespace, reg.Service))
	t.Cleanup(server.Stop)
	hs := httptest.NewServer(server)
	t.Cleanup(hs.Close)
	client, err := erigonrpc.DialContext(t.Context(), hs.URL, log.New())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	var status eezrpc.FollowerStatus
	require.ErrorContains(t, client.CallContext(t.Context(), &status, "eez_followerStatus"), eezrpc.ErrNoFollower.Error())

	_, chain := derivertest.NewL2(t)
	cfg := follower.DefaultConfig
	cfg.Dir = t.TempDir()
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	api.Attach(f)

	require.NoError(t, client.CallContext(t.Context(), &status, "eez_followerStatus"))
	require.Equal(t, hexutil.Uint64(1), status.Cursor)
	require.Equal(t, hexutil.Uint64(l1Block), status.LastSettlement.L1Block)

	var s *eezrpc.Settlement
	require.NoError(t, client.CallContext(t.Context(), &s, "eez_settlement", hexutil.Uint64(1)))
	require.Equal(t, status.LastSettlement, s)
	for _, n := range []hexutil.Uint64{0, 2} {
		s = &eezrpc.Settlement{}
		require.NoError(t, client.CallContext(t.Context(), &s, "eez_settlement", n))
		require.Nil(t, s, "block %d", n)
	}

	input, err := protocol.EncodePostAndVerifyBatch(b.Batch)
	require.NoError(t, err)
	var decoded eezrpc.Batch
	require.NoError(t, client.CallContext(t.Context(), &decoded, "eez_decodeBatch", hexutil.Bytes(input)))
	require.Len(t, decoded.Payload.Blocks, 1)
	require.Equal(t, b.Blocks[0].Transactions()[0].Hash(), decoded.Payload.Blocks[0].Transactions[0])
}

// serve returns a client of api over JSON-RPC.
func serve(t *testing.T, api *eezrpc.API) *erigonrpc.Client {
	t.Helper()
	server := erigonrpc.NewServer(1, false, false, true, log.New(), 0)
	reg := api.API()
	require.NoError(t, server.RegisterName(reg.Namespace, reg.Service))
	t.Cleanup(server.Stop)
	hs := httptest.NewServer(server)
	t.Cleanup(hs.Close)
	client, err := erigonrpc.DialContext(t.Context(), hs.URL, log.New())
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func followerStatus(t *testing.T, client *erigonrpc.Client) map[string]any {
	t.Helper()
	var raw json.RawMessage
	require.NoError(t, client.CallContext(t.Context(), &raw, "eez_followerStatus"))
	var fields map[string]any
	require.NoError(t, json.Unmarshal(raw, &fields))
	return fields
}

// TestFollowerStatus renders the status of a follower before boot, of an
// L1-only follower, and of one that follows signed heads: the derivation
// fields as before, the sync mode and phase, L1's finalized block and root,
// the devp2p and overlay peers, the latest head of each signing source and
// the head's source, null where there is none.
func TestFollowerStatus(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: common.Address{1}, Extra: []byte{}, Txs: [][]byte{c.Transfer(common.Address{2}, 1)}})
	c.Post(derivertest.Posting{Batch: settled})
	f, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.L1.Finalize(f.Number))
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: common.Address{3}, Extra: []byte{}, Txs: [][]byte{}}).Blocks[0]

	api := eezrpc.NewAPI()
	client := serve(t, api)
	attach := func(cfg follower.Config) *follower.Follower {
		_, chain := derivertest.NewL2(t)
		cfg.Dir = t.TempDir()
		fl, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
		require.NoError(t, err)
		api.Attach(fl)
		return fl
	}
	keys := []string{
		"anchor", "cursor", "finalized", "headSource", "l1Finalized", "l1Scanned", "lastSettlement", "overlayPeers",
		"peers", "phase", "safe", "signedHeads", "syncMode",
	}

	attach(follower.DefaultConfig)
	fields := followerStatus(t, client)
	require.Equal(t, keys, slices.Sorted(maps.Keys(fields)))
	require.Equal(t, "booting", fields["phase"])
	require.Equal(t, "l1_only", fields["syncMode"])
	require.Equal(t, "0x0", fields["cursor"])
	for _, k := range []string{"anchor", "lastSettlement", "overlayPeers", "signedHeads", "headSource"} {
		require.Nil(t, fields[k], k)
	}

	require.NoError(t, attach(follower.DefaultConfig).Boot(t.Context()))
	fields = followerStatus(t, client)
	require.Equal(t, keys, slices.Sorted(maps.Keys(fields)))
	require.Equal(t, "0x1", fields["cursor"])
	require.Equal(t, "following", fields["phase"])
	require.Equal(t, map[string]any{
		"l1Block":     hexutil.Uint64(f.Number).String(),
		"l1BlockHash": f.Hash.Hex(),
		"root":        settled.Blocks[0].Hash().Hex(),
		"l2Block":     "0x1",
	}, fields["anchor"])
	require.Equal(t, "0x0", fields["peers"])
	require.Nil(t, fields["overlayPeers"])
	require.Nil(t, fields["signedHeads"])
	require.Equal(t, "derivation", fields["headSource"])

	heads := make(chan *seqsig.VerifiedHead, 1)
	var overlayPeers atomic.Int64
	cfg := follower.DefaultConfig
	cfg.Heads = heads
	cfg.PeerCount = func(context.Context) (uint64, error) { return 3, nil }
	cfg.OverlayPeerCount = func() int { return int(overlayPeers.Load()) }
	fl := attach(cfg)
	require.NoError(t, fl.Boot(t.Context()))
	require.Eventually(t, func() bool { return followerStatus(t, client)["peers"] == "0x3" }, 10*time.Second, time.Millisecond)
	fields = followerStatus(t, client)
	require.Equal(t, "0x0", fields["overlayPeers"])
	require.Equal(t, []any{}, fields["signedHeads"])

	overlayPeers.Store(2)
	signer := common.HexToAddress("0x5e9")
	heads <- &seqsig.VerifiedHead{Signer: signer, Block: unsettled, Head: seqsig.Head{
		RollupID: 1, Number: unsettled.NumberU64(), BlockHash: unsettled.Hash(), ParentHash: unsettled.ParentHash(),
		EpochMs: 1_700_000_000_000, Counter: 7,
	}}
	require.NoError(t, fl.StepOverlay(t.Context()))
	fields = followerStatus(t, client)
	require.Equal(t, keys, slices.Sorted(maps.Keys(fields)))
	require.Equal(t, "0x2", fields["overlayPeers"])
	require.Equal(t, []any{map[string]any{
		"signer":  signer.Hex(),
		"epochMs": "0x18bcfe56800",
		"counter": "0x7",
		"number":  "0x2",
		"hash":    unsettled.Hash().Hex(),
	}}, fields["signedHeads"])
	require.Equal(t, "signed_head", fields["headSource"])
}
