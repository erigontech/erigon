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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestEventLogsParseBack(t *testing.T) {
	g := newValueGen(11)
	registry, root, callHash := g.address(), g.hash(), g.hash()

	l, err := BatchPostedLog(registry, root, []uint64{1, 7})
	require.NoError(t, err)
	require.Equal(t, registry, l.Address)
	posted, err := ParseBatchPosted(l)
	require.NoError(t, err)
	require.Equal(t, &BatchPosted{SharedPublicInput: root, RollupIDs: []uint64{1, 7}}, posted)

	l, err = L2ExecutionPerformedLog(registry, 7, root, big.NewInt(9))
	require.NoError(t, err)
	performed, err := ParseL2ExecutionPerformed(l)
	require.NoError(t, err)
	require.Equal(t, uint64(7), performed.RollupID)
	require.Equal(t, root, performed.NewRoot)
	require.Equal(t, int64(9), performed.EtherBalance.Int64())

	l, err = ExecutionConsumedLog(registry, callHash, 7, big.NewInt(3))
	require.NoError(t, err)
	consumed, err := ParseExecutionConsumed(l)
	require.NoError(t, err)
	require.Equal(t, &ExecutionConsumed{CrossChainCallHash: callHash, RollupID: 7, EntryQueueIndex: big.NewInt(3)}, consumed)

	l, err = L2TxSkippedLog(registry, big.NewInt(2), []byte{0xde, 0xad})
	require.NoError(t, err)
	skipped, err := ParseL2TxSkipped(l)
	require.NoError(t, err)
	require.Equal(t, &L2TxSkipped{EntryIndex: big.NewInt(2), RevertData: []byte{0xde, 0xad}}, skipped)

	executed := &L2CrossChainCallExecuted{
		CrossChainCallHash: callHash,
		Proxy:              g.address(),
		SourceAddress:      g.address(),
		CallData:           []byte{1, 2},
		Value:              big.NewInt(5),
	}
	l, err = L2CrossChainCallExecutedLog(EEZL2Address, executed)
	require.NoError(t, err)
	require.Equal(t, EEZL2Address, l.Address)
	parsed, err := ParseL2CrossChainCallExecuted(l)
	require.NoError(t, err)
	require.Equal(t, executed, parsed)
	require.Equal(t, common.BytesToHash(executed.Proxy[:]), l.Topics[2])
}
