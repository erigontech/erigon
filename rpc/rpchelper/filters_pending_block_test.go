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

package rpchelper

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

func handleHeader(t *testing.T, f *Filters, header *types.Header) {
	t.Helper()
	payload, err := rlp.EncodeToBytes(header)
	require.NoError(t, err)
	f.OnNewEvent(&remoteproto.SubscribeReply{Type: remoteproto.Event_HEADER, Data: payload})
}

func handlePendingBlock(t *testing.T, f *Filters, block *types.Block) {
	t.Helper()
	payload, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	f.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{RplBlock: payload})
}

// TestFilters_PendingBlockInvalidatedByNewHeader pins that a cached pending
// block stops being served once the chain moves without this node building:
// a new header at or above the pending block's height, or a competing block
// replacing its parent, must clear it, while headers that leave it extending
// the head (its own parent, deeper ancestors) must not.
func TestFilters_PendingBlockInvalidatedByNewHeader(t *testing.T) {
	t.Parallel()

	parent := &types.Header{Number: *uint256.NewInt(9), Extra: []byte("parent")}
	pending := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(10), ParentHash: parent.Hash()})

	cases := []struct {
		name     string
		header   *types.Header
		wantKept bool
	}{
		{"deeper ancestor keeps it", &types.Header{Number: *uint256.NewInt(7)}, true},
		{"its own parent keeps it", parent, true},
		{"competing parent clears it", &types.Header{Number: *uint256.NewInt(9), Extra: []byte("competing")}, false},
		{"same height clears it", &types.Header{Number: *uint256.NewInt(10)}, false},
		{"higher head clears it", &types.Header{Number: *uint256.NewInt(14)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := New(t.Context(), FiltersConfig{}, nil, nil, nil, func() {}, log.New(), nil)
			handlePendingBlock(t, f, pending)
			require.Equal(t, pending.Hash(), f.LastPendingBlock().Hash())

			handleHeader(t, f, tc.header)

			if tc.wantKept {
				require.NotNil(t, f.LastPendingBlock())
				require.Equal(t, pending.Hash(), f.LastPendingBlock().Hash())
			} else {
				require.Nil(t, f.LastPendingBlock())
			}
		})
	}
}
