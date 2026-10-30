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

package state_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

type pbinCommitmentReadCounter struct {
	kv.TemporalRwTx
	reads *atomic.Uint64
}

func (tx pbinCommitmentReadCounter) GetLatest(domain kv.Domain, k []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentBinDomain && !bytes.Equal(k, pbt.GlobalRootKey()) && !bytes.Equal(k, commitment.KeyCommitmentState) {
		tx.reads.Add(1)
	}
	return tx.TemporalRwTx.GetLatest(domain, k, opts)
}

type pbinWorkerBarrier struct {
	mu     sync.Mutex
	inside int
	met    chan struct{}
}

func (b *pbinWorkerBarrier) enter() {
	b.mu.Lock()
	b.inside++
	if b.inside == 2 {
		select {
		case <-b.met:
		default:
			close(b.met)
		}
	}
	b.mu.Unlock()
	select {
	case <-b.met:
	case <-time.After(30 * time.Second):
	}
	b.mu.Lock()
	b.inside--
	b.mu.Unlock()
}

type pbinBarrierPin struct {
	kv.TemporalFilesPin
	barrier *pbinWorkerBarrier
}

func (p pbinBarrierPin) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	p.barrier.enter()
	return p.TemporalFilesPin.BeginTemporalRo(ctx)
}

type pbinBarrierTx struct {
	kv.TemporalRwTx
	barrier *pbinWorkerBarrier
}

func (tx pbinBarrierTx) Pin() kv.TemporalFilesPin {
	return pbinBarrierPin{TemporalFilesPin: tx.TemporalRwTx.(interface{ Pin() kv.TemporalFilesPin }).Pin(), barrier: tx.barrier}
}

func TestConvertPBinRunsWalkAndRoundOnSeveralWorkers(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	walkWorkers := temporal.LeafWalkWorkers(t)
	barrier := &pbinWorkerBarrier{met: make(chan struct{})}
	convertPBin(t, db, agg, db, agg, 7, func(options *state.PBinConvertOptions) {
		options.TargetDB = db
		options.TargetTx = pbinBarrierTx{TemporalRwTx: options.TargetTx, barrier: barrier}
		options.Workers = 4
	})
	require.GreaterOrEqual(t, walkWorkers(), 2, "the source walk must run on several workers")
	select {
	case <-barrier.met:
	default:
		t.Fatal("the commitment round must hold two worker contexts open at once")
	}
}

func TestConvertPBinDetachDoesNotReadMDBX(t *testing.T) {
	selectPBinConvertHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertState(t, db)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	var reads atomic.Uint64
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         pbinCommitmentReadCounter{TemporalRwTx: targetTx, reads: &reads},
		TargetDomain:     kv.CommitmentBinDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	require.Zero(t, reads.Load(), "detach must not read previous commitment values")
}
