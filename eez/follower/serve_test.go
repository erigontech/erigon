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

package follower_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// TestServeBlocks: an EEZ chain serves its headers and bodies over devp2p, a
// Sync block's system transaction among them, which is how a reth follower
// fetches the head a sequencer gives it.
func TestServeBlocks(t *testing.T) {
	c := derivertest.NewComposer(t)
	b := c.Compose([]da.Action{{
		SourceRollupID: protocol.MainnetRollupID, TargetRollupID: 1, SourceAddress: bob, TargetAddress: alice,
		Data: []byte{}, Success: true, ReturnData: []byte{},
	}}, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(alice, 1)}})
	c.Post(derivertest.Posting{Batch: b})
	n := newNode(t, c.L1)
	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, b.Blocks...)
	sync := b.Blocks[len(b.Blocks)-1]
	var system bool
	for _, tx := range sync.Transactions() {
		system = system || tx.Type() == protocol.SystemTxType
	}
	require.True(t, system, "the Sync block holds a system transaction")

	ask := func(id sentryproto.MessageId, packet any, want sentryproto.MessageId) []byte {
		t.Helper()
		data, err := rlp.EncodeToBytes(packet)
		require.NoError(t, err)
		n.m.ReceiveWg.Add(1)
		for _, err := range n.m.Send(&sentryproto.InboundMessage{Id: id, Data: data, PeerId: n.m.PeerId}) {
			require.NoError(t, err)
		}
		n.m.ReceiveWg.Wait()
		for i := 0; ; i++ {
			msg, err := n.m.SentMessage(i)
			require.NoError(t, err, "no %s reply", want)
			if msg.Id == want {
				return msg.Data
			}
		}
	}

	var headers eth.BlockHeadersPacket66
	require.NoError(t, rlp.DecodeBytes(ask(sentryproto.MessageId_GET_BLOCK_HEADERS_66, &eth.GetBlockHeadersPacket66{
		RequestId:             1,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{Origin: eth.HashOrNumber{Hash: sync.Hash()}, Amount: 1},
	}, sentryproto.MessageId_BLOCK_HEADERS_66), &headers))
	require.Len(t, headers.BlockHeadersPacket, 1)
	require.Equal(t, sync.Hash(), headers.BlockHeadersPacket[0].Hash())

	var bodies eth.BlockBodiesPacket66
	require.NoError(t, rlp.DecodeBytes(ask(sentryproto.MessageId_GET_BLOCK_BODIES_66, &eth.GetBlockBodiesPacket66{
		RequestId:            2,
		GetBlockBodiesPacket: []common.Hash{sync.Hash()},
	}, sentryproto.MessageId_BLOCK_BODIES_66), &bodies))
	require.Len(t, bodies.BlockBodiesPacket, 1)
	txs := types.Transactions(bodies.BlockBodiesPacket[0].Transactions)
	require.Equal(t, sync.Transactions().Len(), txs.Len())
	require.Equal(t, sync.TxHash(), types.DeriveSha(txs))
}
