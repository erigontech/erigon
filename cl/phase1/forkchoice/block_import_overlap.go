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

package forkchoice

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var (
	blockImportOverlapCounter  = metrics.GetOrCreateCounterVec("caplin_block_import_overlap_total", []string{"result"})
	blockImportOverlapJoinWait = metrics.GetOrCreateSummary("caplin_block_import_overlap_join_wait")
)

type blockImportOverlap struct {
	block              *cltypes.SignedBeaconBlock
	segment            *fork_graph.ChainSegment
	done               chan struct{}
	settled            chan struct{}
	prepared           *preparedPostState
	err                error
	transitionDuration time.Duration
}

func cloneBlockForImport(cfg *clparams.BeaconChainConfig, block *cltypes.SignedBeaconBlock) (*cltypes.SignedBeaconBlock, error) {
	encoded, err := block.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	clone := cltypes.NewSignedBeaconBlock(cfg, block.Version())
	if err := clone.DecodeSSZ(encoded, int(block.Version())); err != nil {
		return nil, err
	}
	return clone, nil
}

func (f *ForkChoiceStore) startBlockImportOverlapLocked(block *cltypes.SignedBeaconBlock, blockRoot common.Hash, fullValidation bool) *blockImportOverlap {
	switch {
	case block.Block.Slot+1 < f.Slot():
		blockImportOverlapCounter.WithLabelValues("skipped_not_tip").Inc()
		return nil
	case f.syncedDataManager.Syncing():
		blockImportOverlapCounter.WithLabelValues("skipped_syncing").Inc()
		return nil
	}
	parentState := f.forkGraph.CurrentStateLocked(block.Block.ParentRoot)
	switch {
	case parentState == nil:
		blockImportOverlapCounter.WithLabelValues("skipped_parent_not_current").Inc()
		return nil
	case f.syncedDataManager.HeadRoot() != block.Block.ParentRoot:
		blockImportOverlapCounter.WithLabelValues("skipped_parent_not_synced_head").Inc()
		return nil
	case block.Block.Slot/f.beaconCfg.SlotsPerEpoch > parentState.Slot()/f.beaconCfg.SlotsPerEpoch:
		blockImportOverlapCounter.WithLabelValues("skipped_epoch_boundary").Inc()
		return nil
	}
	clonedBlock, err := cloneBlockForImport(f.beaconCfg, block)
	if err != nil {
		blockImportOverlapCounter.WithLabelValues("skipped_begin").Inc()
		return nil
	}
	segment, result, err := f.forkGraph.BeginChainSegment(clonedBlock, blockRoot, fullValidation)
	if result == fork_graph.PrecomputedEpochStateAvailable {
		blockImportOverlapCounter.WithLabelValues("skipped_precomputed_epoch_state").Inc()
		return nil
	}
	if err != nil || result != fork_graph.Started {
		blockImportOverlapCounter.WithLabelValues("skipped_begin").Inc()
		return nil
	}
	settled := make(chan struct{})
	if f.blockImportsInFlight == nil {
		f.blockImportsInFlight = make(map[common.Hash]chan struct{})
	}
	f.blockImportsInFlight[blockRoot] = settled
	overlap := &blockImportOverlap{block: clonedBlock, segment: segment, done: make(chan struct{}), settled: settled}
	go func() {
		defer close(overlap.done)
		defer func() {
			if recovered := recover(); recovered != nil {
				overlap.err = fmt.Errorf("block import prepare panic: %v", recovered)
				log.Error("Block import prepare panic", "err", recovered, "stack", string(debug.Stack()))
			}
		}()
		clonedRoot, err := clonedBlock.Block.HashSSZ()
		if err != nil {
			overlap.err = fmt.Errorf("hash cloned block: %w", err)
			return
		}
		if clonedRoot != blockRoot {
			overlap.err = fmt.Errorf("cloned block root mismatch: got %x, want %x", clonedRoot, blockRoot)
			return
		}
		started := time.Now()
		segment.Transition()
		overlap.transitionDuration = time.Since(started)
		if postState := segment.PostState(); postState != nil {
			overlap.prepared = preparePostState(postState, clonedBlock.Block)
			// Warm the hash caches that the addPending* calls read under f.mu at commit.
			_, _ = postState.PendingConsolidations().HashSSZ()
			_, _ = postState.PendingDeposits().HashSSZ()
			_, _ = postState.PendingPartialWithdrawals().HashSSZ()
		}
		if f.blockImportPrepareHook != nil {
			f.blockImportPrepareHook()
		}
	}()
	return overlap
}

func (f *ForkChoiceStore) settleBlockImportOverlapLocked(blockRoot common.Hash, overlap *blockImportOverlap) {
	settled, ok := f.blockImportsInFlight[blockRoot]
	if !ok || settled != overlap.settled {
		return
	}
	delete(f.blockImportsInFlight, blockRoot)
	close(settled)
}

func (o *blockImportOverlap) join() {
	started := time.Now()
	<-o.done
	blockImportOverlapJoinWait.ObserveDuration(started)
}
