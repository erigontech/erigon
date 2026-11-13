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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// A state that runs several transactions keeps their logs, so the event table
// must start at the frame transaction's first log.
func TestTransactionTraceEventsExcludeEarlierTransactions(t *testing.T) {
	ibs := New(NewNoopReader())
	t.Cleanup(ibs.Close)
	earlier, own := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	ibs.AddLog(&types.Log{Address: earlier})
	ctx := ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Mode: types.FramePostTx}}}, uint256.Int{})
	ibs.AddLog(&types.Log{Address: own})
	trace, err := ctx.TransactionTrace(ibs)
	require.NoError(t, err)
	require.Len(t, trace.Events, 1)
	require.Equal(t, own, trace.Events[0].Address)
}
