// Copyright 2024 The Erigon Authors
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

package stages

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

type historyDownloaderStub struct {
	finished          bool
	progress          atomic.Uint64
	requestErr        error
	requestMore       func() error
	skipped           []network.SkippedFullBlock
	recoverySource    bool
	recover           func(context.Context, []network.SkippedFullBlock, map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult
	acknowledged      int
	validateBlock     network.ValidateBlockFn
	validateLookahead network.ValidateLookaheadFn
}

type recoveryBlockReader map[common.Hash]*cltypes.SignedBeaconBlock

func (r recoveryBlockReader) ReadBlockBySlot(context.Context, kv.Tx, uint64) (*cltypes.SignedBeaconBlock, error) {
	return nil, nil
}
func (r recoveryBlockReader) ReadBlockByRoot(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return r[root], nil
}
func (r recoveryBlockReader) ReadHeaderByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	return nil, nil
}
func (r recoveryBlockReader) ReadBeaconBlockBodyBySlot(context.Context, kv.Tx, uint64) (*cltypes.SignedBeaconBlock, error) {
	return nil, nil
}
func (r recoveryBlockReader) FrozenSlots() uint64                                  { return 0 }
func (r recoveryBlockReader) CacheBlockBody(uint64, [][]byte, []*types.Withdrawal) {}

func recoveryGloasBlock(slot uint64, parentRoot, blockHash, parentBlockHash common.Hash) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parentRoot
	block.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = blockHash
	block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = parentBlockHash
	return block
}

func (d *historyDownloaderStub) SetSlotToDownload(uint64)             {}
func (d *historyDownloaderStub) SetExpectedRoot(common.Hash)          {}
func (d *historyDownloaderStub) SetBlockChecker(network.BlockChecker) {}
func (d *historyDownloaderStub) SetOnNewBlock(network.OnNewBlock)     {}
func (d *historyDownloaderStub) SetValidateFunctions(validateBlock network.ValidateBlockFn, validateLookahead network.ValidateLookaheadFn) {
	d.validateBlock = validateBlock
	d.validateLookahead = validateLookahead
}
func (d *historyDownloaderStub) Finished() bool   { return d.finished }
func (d *historyDownloaderStub) Progress() uint64 { return d.progress.Load() }
func (d *historyDownloaderStub) RequestMore(context.Context) error {
	if d.requestMore != nil {
		return d.requestMore()
	}
	return d.requestErr
}
func (d *historyDownloaderStub) SkippedFullBlocks() []network.SkippedFullBlock {
	return d.skipped
}
func (d *historyDownloaderStub) AcknowledgeSkippedFullBlocks(recovered []network.SkippedFullBlock) {
	d.acknowledged += len(recovered)
	pending := make(map[network.SkippedFullBlock]struct{}, len(recovered))
	for _, item := range recovered {
		pending[item] = struct{}{}
	}
	remaining := d.skipped[:0]
	for _, item := range d.skipped {
		if _, ok := pending[item]; !ok {
			remaining = append(remaining, item)
		}
	}
	d.skipped = remaining
}
func (d *historyDownloaderStub) HasEnvelopeRecoverySource() bool { return d.recoverySource }
func (d *historyDownloaderStub) RecoverSkippedEnvelopes(ctx context.Context, skipped []network.SkippedFullBlock, blocks map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult {
	if d.recover != nil {
		return d.recover(ctx, skipped, blocks)
	}
	return network.EnvelopeRecoveryResult{}
}
func (d *historyDownloaderStub) SetThrottle(time.Duration) {}
func (d *historyDownloaderStub) SetNeverSkip(bool)         {}

func TestSpawnStageHistoryDownloadReturnsDownloaderFailure(t *testing.T) {
	wantErr := errors.New("terminal downloader failure")
	downloader := &historyDownloaderStub{requestErr: wantErr}
	downloader.progress.Store(math.MaxUint64)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:  &clparams.MainnetBeaconConfig,
		downloader: downloader,
	}, ctx, log.New())
	if !errors.Is(err, wantErr) {
		t.Fatalf("SpawnStageHistoryDownload() error = %v, want %v", err, wantErr)
	}
}

func TestSpawnStageHistoryDownloadReturnsFailureWhenRequestCrossesELFloor(t *testing.T) {
	wantErr := errors.New("terminal downloader failure at EL floor")
	destinationSlot := clparams.MainnetBeaconConfig.BellatrixForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
	downloader := &historyDownloaderStub{}
	downloader.progress.Store(destinationSlot + 1)
	downloader.requestMore = func() error {
		downloader.progress.Store(destinationSlot)
		return wantErr
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:  &clparams.MainnetBeaconConfig,
		downloader: downloader,
		engine:     &testExecutionEngine{supportInsertion: true},
	}, ctx, log.New())
	if !errors.Is(err, wantErr) {
		t.Fatalf("SpawnStageHistoryDownload() error = %v, want %v", err, wantErr)
	}
}

func TestSpawnStageHistoryDownloadReturnsEnvelopeRecoveryFailure(t *testing.T) {
	downloader := &historyDownloaderStub{
		finished: true,
		skipped:  []network.SkippedFullBlock{{Slot: 1}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:  &clparams.MainnetBeaconConfig,
		downloader: downloader,
	}, ctx, log.New())
	if err == nil {
		t.Fatal("SpawnStageHistoryDownload() returned nil after envelope recovery failed")
	}
}

func TestConfigureHistoryEnvelopeAdmissionPreservesPreinstalledValidatorsWithoutForkchoice(t *testing.T) {
	downloader := &historyDownloaderStub{}
	blockCalls := 0
	lookaheadCalls := 0
	downloader.SetValidateFunctions(
		func(*cltypes.SignedBeaconBlock) error {
			blockCalls++
			return nil
		},
		func(*cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock) error {
			lookaheadCalls++
			return nil
		},
	)

	configureHistoryEnvelopeAdmission(StageHistoryReconstructionCfg{downloader: downloader})

	require.NoError(t, downloader.validateBlock(nil))
	require.NoError(t, downloader.validateLookahead(nil, nil))
	require.Equal(t, 1, blockCalls)
	require.Equal(t, 1, lookaheadCalls)
}

func TestUnresolvedSkippedEnvelopesRetriesEveryMissingEnvelope(t *testing.T) {
	first := network.SkippedFullBlock{Slot: 1, Root: [32]byte{1}}
	second := network.SkippedFullBlock{Slot: 2, Root: [32]byte{2}}
	result := network.EnvelopeRecoveryResult{}

	remaining := unresolvedSkippedEnvelopes([]network.SkippedFullBlock{first, second}, result, func(network.SkippedFullBlock, *cltypes.SignedExecutionPayloadEnvelope) bool {
		t.Fatal("missing envelopes must not be persisted")
		return false
	})

	if len(remaining) != 2 || remaining[0] != first || remaining[1] != second {
		t.Fatalf("remaining = %v, want both missing envelopes", remaining)
	}
}

func TestRecoverSkippedEnvelopeBatchCanonicalChildProvesEmptyWithoutEnvelope(t *testing.T) {
	parent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	candidateChild := recoveryGloasBlock(101, parentRoot, common.Hash{4}, common.Hash{5})
	candidateChildRoot, err := candidateChild.Block.HashSSZ()
	require.NoError(t, err)
	canonicalChild := recoveryGloasBlock(parent.Block.Slot+65, parentRoot, common.Hash{6}, common.Hash{7})
	canonicalChildRoot, err := canonicalChild.Block.HashSSZ()
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, canonicalChild.Block.Slot, canonicalChildRoot)
	}))
	recoveryCalled := false
	downloader := &historyDownloaderStub{
		recoverySource: true,
		recover: func(context.Context, []network.SkippedFullBlock, map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult {
			recoveryCalled = true
			return network.EnvelopeRecoveryResult{}
		},
	}
	item := network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: canonicalChild.Block.Slot, ChildRoot: candidateChildRoot}
	reader := recoveryBlockReader{parentRoot: parent, candidateChildRoot: candidateChild, canonicalChildRoot: canonicalChild}
	cfg := StageHistoryReconstructionCfg{indiciesDB: db, blockReader: reader, downloader: downloader}

	remaining := recoverSkippedEnvelopeBatch(t.Context(), t.Context(), cfg, []network.SkippedFullBlock{item})

	require.Empty(t, remaining)
	require.False(t, recoveryCalled)
}

func TestRecoverSkippedEnvelopeBatchDistantCanonicalChildProvesFull(t *testing.T) {
	beaconCfg, _, bid, envelope, _ := validAnchorEnvelopeFixture(t, 0)
	parent := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	parent.Block.Slot = bid.Slot
	parent.Block.Body.SignedExecutionPayloadBid.Message = bid
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = parentRoot
	canonicalChild := recoveryGloasBlock(parent.Block.Slot+65, parentRoot, common.Hash{4}, bid.BlockHash)
	canonicalChildRoot, err := canonicalChild.Block.HashSSZ()
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, canonicalChild.Block.Slot, canonicalChildRoot)
	}))
	recoveryCalled := false
	downloader := &historyDownloaderStub{
		recoverySource: true,
		recover: func(_ context.Context, skipped []network.SkippedFullBlock, _ map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult {
			recoveryCalled = true
			require.Len(t, skipped, 1)
			return network.EnvelopeRecoveryResult{Envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{parentRoot: envelope}}
		},
	}
	item := network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: canonicalChild.Block.Slot, ChildRoot: common.Hash{9}}
	reader := recoveryBlockReader{parentRoot: parent, canonicalChildRoot: canonicalChild}
	cfg := StageHistoryReconstructionCfg{beaconCfg: beaconCfg, indiciesDB: db, blockReader: reader, downloader: downloader}

	remaining := recoverSkippedEnvelopeBatch(t.Context(), t.Context(), cfg, []network.SkippedFullBlock{item})

	require.Empty(t, remaining)
	require.True(t, recoveryCalled)
}

func TestCanonicalSkippedEnvelopeSearchRejectsNonDirectDescendant(t *testing.T) {
	parent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	directChild := recoveryGloasBlock(101, parentRoot, common.Hash{4}, common.Hash{5})
	directChildRoot, err := directChild.Block.HashSSZ()
	require.NoError(t, err)
	descendant := recoveryGloasBlock(102, directChildRoot, common.Hash{6}, common.Hash{7})

	full, known, err := findCanonicalSkippedEnvelopeAvailability(
		t.Context(),
		network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: descendant.Block.Slot},
		parent,
		func(_ context.Context, slot uint64) (*cltypes.SignedBeaconBlock, bool, error) {
			if slot == descendant.Block.Slot {
				return descendant, true, nil
			}
			return nil, false, nil
		},
	)

	require.NoError(t, err)
	require.False(t, full)
	require.False(t, known)
}

func TestCanonicalSkippedEnvelopeSearchFindsAlternateSlotFull(t *testing.T) {
	parent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	canonicalChild := recoveryGloasBlock(102, parentRoot, common.Hash{4}, common.Hash{2})

	full, known, err := findCanonicalSkippedEnvelopeAvailability(
		t.Context(),
		network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: parent.Block.Slot + 1},
		parent,
		func(_ context.Context, slot uint64) (*cltypes.SignedBeaconBlock, bool, error) {
			if slot == canonicalChild.Block.Slot {
				return canonicalChild, true, nil
			}
			return nil, false, nil
		},
	)

	require.NoError(t, err)
	require.True(t, full)
	require.True(t, known)
}

func TestCanonicalSkippedEnvelopeSearchInvalidHugeHintBoundsCanonicalReads(t *testing.T) {
	parent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	reads := 0

	full, known, err := findCanonicalSkippedEnvelopeAvailability(
		t.Context(),
		network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: math.MaxUint64},
		parent,
		func(context.Context, uint64) (*cltypes.SignedBeaconBlock, bool, error) {
			reads++
			return nil, false, nil
		},
	)

	require.NoError(t, err)
	require.False(t, full)
	require.False(t, known)
	require.Equal(t, skippedEnvelopeCanonicalFallbackSlots+1, reads)
}

func TestCanonicalSkippedEnvelopeSearchMaxAnchorDoesNotRead(t *testing.T) {
	parent := recoveryGloasBlock(math.MaxUint64, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	reads := 0

	full, known, err := findCanonicalSkippedEnvelopeAvailability(
		t.Context(),
		network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot},
		parent,
		func(context.Context, uint64) (*cltypes.SignedBeaconBlock, bool, error) {
			reads++
			return nil, false, nil
		},
	)

	require.NoError(t, err)
	require.False(t, full)
	require.False(t, known)
	require.Zero(t, reads)
}

func TestCanonicalSkippedEnvelopeSearchNearMaxAnchorBoundsReads(t *testing.T) {
	parent := recoveryGloasBlock(math.MaxUint64-63, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	reads := 0

	full, known, err := findCanonicalSkippedEnvelopeAvailability(
		t.Context(),
		network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot},
		parent,
		func(context.Context, uint64) (*cltypes.SignedBeaconBlock, bool, error) {
			reads++
			return nil, false, nil
		},
	)

	require.NoError(t, err)
	require.False(t, full)
	require.False(t, known)
	require.Equal(t, 63, reads)
}

func TestCanonicalSkippedEnvelopeSearchStopsOnCancellation(t *testing.T) {
	parent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err = findCanonicalSkippedEnvelopeAvailability(
		ctx,
		network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot},
		parent,
		func(context.Context, uint64) (*cltypes.SignedBeaconBlock, bool, error) {
			t.Fatal("canceled search must not read canonical slots")
			return nil, false, nil
		},
	)

	require.ErrorIs(t, err, context.Canceled)
}

func TestRecoverSkippedEnvelopeBatchUnknownCannotPersistLateEnvelope(t *testing.T) {
	beaconCfg, _, bid, lateEnvelope, _ := validAnchorEnvelopeFixture(t, 0)
	parent := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	parent.Block.Slot = bid.Slot
	parent.Block.Body.SignedExecutionPayloadBid.Message = bid
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	lateEnvelope.Message.BeaconBlockRoot = parentRoot
	require.NoError(t, network.ValidateFetchedEnvelope(beaconCfg, parent, parentRoot, lateEnvelope))
	sideChild := recoveryGloasBlock(parent.Block.Slot+2, parentRoot, common.Hash{4}, common.Hash{5})
	sideChildRoot, err := sideChild.Block.HashSSZ()
	require.NoError(t, err)
	canonicalChild := recoveryGloasBlock(parent.Block.Slot+2, common.Hash{9}, common.Hash{6}, common.Hash{7})
	canonicalChildRoot, err := canonicalChild.Block.HashSSZ()
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, canonicalChild.Block.Slot, canonicalChildRoot)
	}))
	recoveryCalled := false
	downloader := &historyDownloaderStub{
		recoverySource: true,
		recover: func(_ context.Context, skipped []network.SkippedFullBlock, _ map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult {
			recoveryCalled = true
			return network.EnvelopeRecoveryResult{Envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
				parentRoot: lateEnvelope,
			}}
		},
	}
	item := network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: sideChild.Block.Slot, ChildRoot: sideChildRoot}
	reader := recoveryBlockReader{parentRoot: parent, sideChildRoot: sideChild, canonicalChildRoot: canonicalChild}
	cfg := StageHistoryReconstructionCfg{beaconCfg: beaconCfg, indiciesDB: db, blockReader: reader, downloader: downloader}

	remaining := recoverSkippedEnvelopeBatch(t.Context(), t.Context(), cfg, []network.SkippedFullBlock{item})

	require.Equal(t, []network.SkippedFullBlock{item}, remaining)
	require.False(t, recoveryCalled)
}

func TestRequestMoreRecoversCapacityBeforeDownloaderFinishes(t *testing.T) {
	parent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	child := recoveryGloasBlock(102, parentRoot, common.Hash{4}, common.Hash{5})
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, child.Block.Slot, childRoot)
	}))
	item := network.SkippedFullBlock{Slot: parent.Block.Slot, Root: parentRoot, ChildSlot: child.Block.Slot, ChildRoot: childRoot}
	downloader := &historyDownloaderStub{
		requestErr:     network.ErrSkippedEnvelopeRecoveryCapacity,
		skipped:        []network.SkippedFullBlock{item},
		recoverySource: true,
	}
	cfg := StageHistoryReconstructionCfg{
		downloader:  downloader,
		indiciesDB:  db,
		blockReader: recoveryBlockReader{parentRoot: parent, childRoot: child},
	}

	require.NoError(t, requestMoreWithEnvelopeRecovery(t.Context(), cfg))
	require.Equal(t, 1, downloader.acknowledged)
	require.Empty(t, downloader.skipped)
}

func TestRequestMoreAcknowledgesPartialCapacityRecovery(t *testing.T) {
	resolvedParent := recoveryGloasBlock(100, common.Hash{1}, common.Hash{2}, common.Hash{3})
	resolvedRoot, err := resolvedParent.Block.HashSSZ()
	require.NoError(t, err)
	resolvedChild := recoveryGloasBlock(102, resolvedRoot, common.Hash{4}, common.Hash{5})
	resolvedChildRoot, err := resolvedChild.Block.HashSSZ()
	require.NoError(t, err)
	unresolvedParent := recoveryGloasBlock(99, common.Hash{6}, common.Hash{7}, common.Hash{8})
	unresolvedRoot, err := unresolvedParent.Block.HashSSZ()
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, resolvedChild.Block.Slot, resolvedChildRoot)
	}))
	resolved := network.SkippedFullBlock{Slot: resolvedParent.Block.Slot, Root: resolvedRoot, ChildSlot: resolvedChild.Block.Slot, ChildRoot: common.Hash{9}}
	unresolved := network.SkippedFullBlock{Slot: unresolvedParent.Block.Slot, Root: unresolvedRoot, ChildSlot: 101, ChildRoot: common.Hash{10}}
	downloader := &historyDownloaderStub{
		requestErr:     network.ErrSkippedEnvelopeRecoveryCapacity,
		skipped:        []network.SkippedFullBlock{resolved, unresolved},
		recoverySource: true,
	}
	cfg := StageHistoryReconstructionCfg{
		downloader: downloader,
		indiciesDB: db,
		blockReader: recoveryBlockReader{
			resolvedRoot:      resolvedParent,
			resolvedChildRoot: resolvedChild,
			unresolvedRoot:    unresolvedParent,
		},
	}

	require.NoError(t, requestMoreWithEnvelopeRecoveryPolicy(t.Context(), cfg, 0))
	require.Equal(t, 1, downloader.acknowledged)
	require.Equal(t, []network.SkippedFullBlock{unresolved}, downloader.skipped)

	downloader.requestErr = nil
	require.NoError(t, requestMoreWithEnvelopeRecoveryPolicy(t.Context(), cfg, 0))
}

func TestRecoverSkippedEnvelopeBatchesDoesNotStarveLaterBatches(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Slot: 1}, {Slot: 2}, {Slot: 3}, {Slot: 4}, {Slot: 5}, {Slot: 6}, {Slot: 7}, {Slot: 8}}
	attempted := make([]uint64, 0, len(skipped))
	recoverBatch := func(ctx, _ context.Context, batch []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempted = append(attempted, batch[0].Slot)
		if batch[0].Slot < 7 {
			<-ctx.Done()
			return batch
		}
		return nil
	}

	pending := recoverSkippedEnvelopeBatches(context.Background(), skipped, 2, time.Millisecond, recoverBatch)
	if len(attempted) != 4 || attempted[3] != 7 {
		t.Fatalf("attempted batch starts = %v, want [1 3 5 7]", attempted)
	}
	for _, item := range pending {
		if item.Slot >= 7 {
			t.Fatalf("later recoverable item %d remained pending", item.Slot)
		}
	}
}

func TestRecoverSkippedEnvelopeBatchesKeepsPartialSuccess(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Slot: 1}, {Slot: 2}}
	recoverBatch := func(fetchCtx, persistCtx context.Context, batch []network.SkippedFullBlock) []network.SkippedFullBlock {
		<-fetchCtx.Done()
		if persistCtx.Err() != nil {
			t.Fatalf("persist context expired with fetch context: %v", persistCtx.Err())
		}
		return batch[1:]
	}

	pending := recoverSkippedEnvelopeBatches(context.Background(), skipped, 2, time.Millisecond, recoverBatch)
	if len(pending) != 1 || pending[0].Slot != 2 {
		t.Fatalf("pending = %v, want only slot 2", pending)
	}
}

func TestRecoverSkippedEnvelopesWithoutSourcesDoesNotCompleteBackfill(t *testing.T) {
	cfg := StageHistoryReconstructionCfg{downloader: &network.BackwardBeaconDownloader{}}
	pending := recoverSkippedEnvelopesWithRetryInterval(context.Background(), cfg, []network.SkippedFullBlock{{Slot: 1}}, 0)
	if len(pending) == 0 {
		t.Fatal("recovery without an HTTP or P2P source must not report completion")
	}
}

func TestRecoverSkippedEnvelopesRetriesBeyondThreeAttemptCapacity(t *testing.T) {
	const itemsPerAttempt = int(skippedEnvelopeRecoveryAttemptTimeout/skippedEnvelopeRecoveryBatchTimeout) * skippedEnvelopeRecoveryBatchSize
	skipped := make([]network.SkippedFullBlock, itemsPerAttempt*3+1)
	for i := range skipped {
		skipped[i].Slot = uint64(i + 1)
	}

	attemptStarts := make([]uint64, 0, 4)
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attemptStarts = append(attemptStarts, pending[0].Slot)
		return pending[min(itemsPerAttempt, len(pending)):]
	}

	if pending := recoverSkippedEnvelopesUntilComplete(context.Background(), skipped, recoverAttempt, 0); len(pending) != 0 {
		t.Fatal("recovery stopped before all pending envelopes were recovered")
	}
	if len(attemptStarts) != 4 || attemptStarts[3] != uint64(itemsPerAttempt*3+1) {
		t.Fatalf("attempt starts = %v, want a fourth attempt starting at slot %d", attemptStarts, itemsPerAttempt*3+1)
	}
}

func TestRecoverSkippedEnvelopesStopsWhenParentContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempts++
		cancel()
		return pending
	}

	if pending := recoverSkippedEnvelopesUntilComplete(ctx, []network.SkippedFullBlock{{Slot: 1}}, recoverAttempt, time.Hour); len(pending) == 0 {
		t.Fatal("recovery reported completion with a pending envelope after parent cancellation")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRecoverSkippedEnvelopesStopsAfterBoundedZeroProgress(t *testing.T) {
	attempts := 0
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempts++
		return pending
	}

	if pending := recoverSkippedEnvelopesUntilComplete(t.Context(), []network.SkippedFullBlock{{Slot: 1}}, recoverAttempt, 0); len(pending) == 0 {
		t.Fatal("zero-progress recovery must return an explicit incomplete result")
	}
	if attempts != skippedEnvelopeRecoverySweeps {
		t.Fatalf("attempts = %d, want %d bounded attempts", attempts, skippedEnvelopeRecoverySweeps)
	}
}

func TestRecoverSkippedEnvelopesPartialProgressDoesNotResetBudget(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Slot: 1}, {Slot: 2}}
	attempts := 0
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempts++
		if attempts == 1 {
			return pending[1:]
		}
		return pending
	}

	if pending := recoverSkippedEnvelopesUntilComplete(t.Context(), skipped, recoverAttempt, 0); len(pending) == 0 {
		t.Fatal("one recovered item must not turn an unrelated permanently missing item into success")
	}
	if attempts != skippedEnvelopeRecoverySweeps {
		t.Fatalf("attempts = %d, want %d fixed attempts", attempts, skippedEnvelopeRecoverySweeps)
	}
}

// clampProgress must never report a total below processed nor underflow, even
// when the floor and current counters drift past the frozen highestBlockSeen.
// The last case mirrors the field report where the live EL head advanced past
// the frozen top and previously underflowed the denominator to ~2^64.
func TestClampProgress(t *testing.T) {
	cases := []struct {
		name                     string
		highest, floor, current  uint64
		wantProcessed, wantTotal uint64
	}{
		{"normal", 100, 20, 60, 40, 80},
		{"floor above top", 100, 150, 60, 40, 40},
		{"current above top", 100, 20, 200, 0, 80},
		{"current below floor grows total", 100, 20, 5, 95, 95},
		{"el head past frozen tip", 23_000_000, 23_123_953, 22_983_559, 16_441, 16_441},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			processed, total := clampProgress(tc.highest, tc.floor, tc.current)
			if processed != tc.wantProcessed || total != tc.wantTotal {
				t.Fatalf("clampProgress(%d,%d,%d) = (%d,%d), want (%d,%d)",
					tc.highest, tc.floor, tc.current, processed, total, tc.wantProcessed, tc.wantTotal)
			}
			if processed > total {
				t.Fatalf("processed (%d) exceeds total (%d)", processed, total)
			}
		})
	}
}

// Post-merge the EL block number exceeds the beacon slot, so a snapshot-gap
// floor must be compared against EL block progress, not the slot.
func TestELBackfillFinished_GapUsesBlockNotSlot(t *testing.T) {
	const (
		bellatrixSlot = uint64(4_636_672) // a real beacon-slot floor
		frozenBlock   = uint64(25_073_000)
		headSlot      = uint64(14_460_640)
		headBlock     = uint64(25_224_522)
	)
	destBlock := frozenBlock - 1

	if elBackfillFinished(headSlot, headBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill reported finished at the tip (slot=%d block=%d) while gap down to block %d is unfilled",
			headSlot, headBlock, destBlock)
	}

	// Once EL block progress has descended to the frozen tip, it is finished.
	if !elBackfillFinished(headSlot-150_000, destBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill should be finished once EL block progress reaches the frozen tip (block %d)", destBlock)
	}
}

// Without a snapshot gap, the EL block floor is unset and completion is driven
// purely by the beacon-slot floor (normal Deneb backfill toward the merge).
func TestELBackfillFinished_NoGapUsesSlotFloor(t *testing.T) {
	const bellatrixSlot = uint64(4_636_672)
	noBlockFloor := uint64(math.MaxUint64)

	if elBackfillFinished(bellatrixSlot+1, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must continue while still above the beacon-slot floor")
	}
	if !elBackfillFinished(bellatrixSlot, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must finish once the beacon-slot floor is reached")
	}
}
