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
package deriver

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
)

// hashChain is a local chain whose block n has hash {n+1}.
type hashChain uint64

func blockHash(n uint64) common.Hash { return common.BigToHash(new(big.Int).SetUint64(n + 1)) }

func (c hashChain) CanonicalHash(_ context.Context, n uint64) (common.Hash, bool, error) {
	return blockHash(n), n <= uint64(c), nil
}

func (c hashChain) CanonicalNumber(_ context.Context, h common.Hash) (uint64, bool, error) {
	n := new(big.Int).SetBytes(h[:]).Uint64()
	return n - 1, n > 0 && n-1 <= uint64(c) && blockHash(n-1) == h, nil
}

func TestDecodePayload(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)

	p, err := DecodePayload(b, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(len(w.Blocks)), p.Blocks())
	require.Empty(t, p.Actions)
	require.Empty(t, p.Span.Transactions)

	p, err = DecodePayload(b, 2)
	require.NoError(t, err)
	require.Nil(t, p, "another rollup's payload")

	b.BlobIndices = []*big.Int{new(big.Int)}
	_, err = DecodePayload(b, 1)
	require.ErrorIs(t, err, ErrBlobPayload)
}

func TestSelectRange(t *testing.T) {
	ctx := context.Background()
	chain := hashChain(20)
	settle := func(entry uint64, resumed bool) *protocol.Settlement {
		s := &protocol.Settlement{EntryState: blockHash(entry), FinalState: common.Hash{0xff}, Applied: []protocol.AppliedEntry{{}}}
		if resumed {
			s.Applied[0].Index = 1
		}
		return s
	}
	for _, tc := range []struct {
		name          string
		cursor, entry uint64
		resumed       bool
		want          Range
	}{
		{"fresh at the cursor", 12, 12, false, Range{Anchor: 12, From: 13, To: 18}},
		{"anchored below the cursor", 12, 8, false, Range{Anchor: 8, From: 9, To: 14}},
		{"entry state beyond the window", 12, 5, false, Range{Anchor: 12, From: 13, To: 18}},
		{"entry state above the cursor", 12, 15, false, Range{Anchor: 12, From: 13, To: 18}},
		{"resumed at its Sync block", 12, 12, true, Range{Anchor: 12, From: 12, To: 12, Resumed: true}},
		{"window reaching genesis", 3, 0, false, Range{Anchor: 0, From: 1, To: 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SelectRange(ctx, chain, tc.cursor, 6, settle(tc.entry, tc.resumed))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	unknown := &protocol.Settlement{EntryState: common.Hash{0xee}, Applied: []protocol.AppliedEntry{{}}}
	got, err := SelectRange(ctx, chain, 12, 6, unknown)
	require.NoError(t, err)
	require.Equal(t, Range{Anchor: 12, From: 13, To: 18}, got)
}

func TestCursorAccepted(t *testing.T) {
	s := &protocol.Settlement{EntryState: common.Hash{1}, FinalState: common.Hash{2}}
	require.True(t, CursorAccepted(common.Hash{1}, s), "not derived yet")
	require.True(t, CursorAccepted(common.Hash{2}, s), "derived already")
	require.False(t, CursorAccepted(common.Hash{3}, s))
}
