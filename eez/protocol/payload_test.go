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
package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// TestWindow84DAPayload is T-VEC-4 on the devnet capture: the batch callData
// is a whole stream whose one RollupOperation carries the span of exactly the
// blocks the batch settles, with their beneficiaries, extraData and
// transactions. V0 has one encoding per span, so the callData is the encoding
// of those blocks.
func TestWindow84DAPayload(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	msgs, err := da.Decode(nil, b.CallData)
	require.NoError(t, err)
	p, err := da.ParsePayload(msgs)
	require.NoError(t, err)
	require.Equal(t, b.RollupIDsWithProofSystems[0].RollupID, p.RollupID)
	require.Empty(t, p.Actions)
	s, err := da.DecodeSpan(p.Operations)
	require.NoError(t, err)

	require.Len(t, s.TxCounts, len(w.Blocks))
	var txs [][]byte
	for i, wb := range w.Blocks {
		var block types.Block
		require.NoError(t, rlp.DecodeBytes(wb.RLP, &block))
		require.Equal(t, uint32(len(block.Transactions())), s.TxCounts[i], "block %d", wb.Number)
		require.Equal(t, block.Coinbase(), s.Beneficiaries[i], "block %d", wb.Number)
		require.True(t, bytes.Equal(block.Extra(), s.ExtraData[i]), "block %d extraData", wb.Number)
		for _, tx := range block.Transactions() {
			var buf bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&buf))
			txs = append(txs, buf.Bytes())
		}
	}
	require.Equal(t, len(txs), len(s.Transactions))
	for i := range txs {
		require.Equal(t, txs[i], s.Transactions[i])
	}
}
