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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type finalityContextStub struct {
	finalisedBlockNum uint64
	lastBlockInStep   uint64
	lastBlockInDB     uint64
	lastTxInDB        uint64
	maxReorgDepth     uint64
	ready             bool
}

var unboundedFinalityCtx = finalityContextStub{ready: true}

func (c finalityContextStub) PruneToBlockNum() uint64 {
	return 0
}

func (c finalityContextStub) RetireToBlockNum() uint64 {
	return 0
}

func (c finalityContextStub) MaxReorgDepth() uint64 {
	return c.maxReorgDepth
}

func (c finalityContextStub) ReadyForCollation(_ context.Context, _ kv.RoDB, _ uint64) (finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB uint64, ok bool, err error) {
	return c.finalisedBlockNum, c.lastBlockInStep, c.lastBlockInDB, c.lastTxInDB, c.ready, nil
}

func TestReadyForCollationPreservesFinalityContextResult(t *testing.T) {
	_, agg := testDbAndAggregatorv3(t, 10)
	finalityCtx := finalityContextStub{
		finalisedBlockNum: 12,
		lastBlockInStep:   10,
		lastBlockInDB:     20,
		lastTxInDB:        25,
		ready:             true,
	}
	finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB, ready, err := agg.readyForCollation(t.Context(), kv.Step(0), finalityCtx)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, uint64(12), finalisedBlockNum)
	require.Equal(t, uint64(10), lastBlockInStep)
	require.Equal(t, uint64(20), lastBlockInDB)
	require.Equal(t, uint64(25), lastTxInDB)
}
