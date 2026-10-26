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
	"encoding/json"
	"net/http/httptest"
	"slices"
	"testing"

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
	"github.com/erigontech/erigon/eez/systemtx"
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
