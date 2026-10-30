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
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/stretchr/testify/require"
)

type recordingBlobHistoryDownloader struct {
	mu       sync.Mutex
	headSlot uint64
	notify   *network.BlobBackfilledNotifier
	started  bool
}

func (d *recordingBlobHistoryDownloader) SetHeadSlot(slot uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.headSlot = slot
}

func (d *recordingBlobHistoryDownloader) SetNotifyBlobBackfilled(notify *network.BlobBackfilledNotifier) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.notify = notify
}

func (d *recordingBlobHistoryDownloader) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.started = true
}

func TestBlobHistoryDownloadStartsOnlyAfterBlockHistoryFinishes(t *testing.T) {
	downloader := &recordingBlobHistoryDownloader{}

	startBlobHistoryDownload(false, downloader, 99, func(bool) {})
	require.False(t, downloader.started)
	require.Zero(t, downloader.headSlot)
	require.Nil(t, downloader.notify)

	startBlobHistoryDownload(true, downloader, 99, func(bool) {})
	require.True(t, downloader.started)
	require.Equal(t, uint64(99), downloader.headSlot)
	require.NotNil(t, downloader.notify)
}

func TestBlobHistoryDownloadIgnoresNilConfiguredDownloader(t *testing.T) {
	var downloader *network.BlobHistoryDownloader
	require.NotPanics(t, func() {
		startBlobHistoryDownload(true, downloader, 99, func(bool) {})
	})
}

type failingHistoryDownloader struct {
	err                              error
	finished                         bool
	gloasEnvelopeSignatureVerifiable bool
}

func (d *failingHistoryDownloader) Finished() bool                             { return d.finished }
func (*failingHistoryDownloader) Processed() uint64                            { return 0 }
func (*failingHistoryDownloader) Progress() uint64                             { return 1 }
func (d *failingHistoryDownloader) RequestMore(context.Context) error          { return d.err }
func (*failingHistoryDownloader) SetBlockChecker(network.BlockChecker)         {}
func (*failingHistoryDownloader) SetBlockReader(network.BeaconBlockBodyReader) {}
func (*failingHistoryDownloader) SetExpectedRoot(common.Hash)                  {}
func (*failingHistoryDownloader) SetNeverSkip(bool)                            {}
func (*failingHistoryDownloader) SetOnNewBlock(network.OnNewBlock)             {}
func (*failingHistoryDownloader) SetOnInitialGloasBlock(common.Hash, func(*cltypes.SignedBeaconBlock) error) {
}
func (*failingHistoryDownloader) SetSlotToDownload(uint64)  {}
func (*failingHistoryDownloader) SetThrottle(time.Duration) {}
func (d *failingHistoryDownloader) GloasEnvelopeSignatureVerifiable(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) bool {
	return d.gloasEnvelopeSignatureVerifiable
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

type countingHistoryDownloader struct {
	failingHistoryDownloader
	progress, processed uint64
}

func (d *countingHistoryDownloader) Processed() uint64 { return d.processed }
func (d *countingHistoryDownloader) Progress() uint64  { return d.progress }

func TestHistoryDownloadRateIgnoresSkippedSlots(t *testing.T) {
	d := &countingHistoryDownloader{progress: 20, processed: 5}
	start := time.Unix(1000, 0)
	now := start.Add(10 * time.Second)

	d.progress = 9
	speed, nextStart, nextProcessed := historyDownloadRate(d, start, 5, now)
	require.Zero(t, speed, "skipped slots must not count as speed")
	require.Equal(t, start, nextStart)
	require.Equal(t, uint64(5), nextProcessed)

	d.processed = 25
	speed, _, _ = historyDownloadRate(d, start, 5, now)
	require.InDelta(t, 2.0, speed, 1e-9)
}

func TestHistoryDownloadRateRestartsAboveThousandPerSecond(t *testing.T) {
	d := &countingHistoryDownloader{processed: 20_005}
	start := time.Unix(1000, 0)
	now := start.Add(10 * time.Second)

	speed, nextStart, nextProcessed := historyDownloadRate(d, start, 5, now)
	require.Zero(t, speed)
	require.Equal(t, now, nextStart)
	require.Equal(t, uint64(20_005), nextProcessed)
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

func TestSpawnStageHistoryDownloadReturnsDownloaderFailure(t *testing.T) {
	wantErr := errors.New("canonical successor unavailable")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:    &clparams.MainnetBeaconConfig,
		downloader:   &failingHistoryDownloader{err: wantErr},
		startingSlot: 1,
	}, ctx, log.Root())
	require.ErrorIs(t, err, wantErr)
}

func TestWaitForHistoryDownloadJoinsFinishedWorker(t *testing.T) {
	wantErr := errors.New("commit history progress")
	historyDone := make(chan error, 1)
	historyDone <- wantErr

	err := waitForHistoryDownload(t.Context(), StageHistoryReconstructionCfg{
		downloader: &failingHistoryDownloader{finished: true},
	}, 0, historyDone)
	require.ErrorIs(t, err, wantErr)
}

type initialGloasHistoryDownloader struct {
	failingHistoryDownloader
	block   *cltypes.SignedBeaconBlock
	persist func(*cltypes.SignedBeaconBlock) error
}

type oneBlockHistoryDownloader struct {
	failingHistoryDownloader
	block    *cltypes.SignedBeaconBlock
	envelope *cltypes.SignedExecutionPayloadEnvelope
	onBlock  network.OnNewBlock
}

func (d *oneBlockHistoryDownloader) SetOnNewBlock(onBlock network.OnNewBlock) {
	d.onBlock = onBlock
}

func (d *oneBlockHistoryDownloader) RequestMore(context.Context) error {
	if d.onBlock == nil {
		return errors.New("new block callback was not configured")
	}
	if _, err := d.onBlock(d.block, d.envelope); err != nil {
		return err
	}
	return d.err
}

func (d *initialGloasHistoryDownloader) SetOnInitialGloasBlock(_ common.Hash, persist func(*cltypes.SignedBeaconBlock) error) {
	d.persist = persist
}

func (d *initialGloasHistoryDownloader) RequestMore(context.Context) error {
	if d.persist == nil {
		return errors.New("initial Gloas callback was not configured")
	}
	if err := d.persist(d.block); err != nil {
		return err
	}
	return d.err
}

func TestHistoryDownloadPersistsInitialGloasBlockWithoutPayloadClassification(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = 10
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	stop := errors.New("stop after anchor persistence")
	downloader := &initialGloasHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}, block: block}
	err = SpawnStageHistoryDownload(StageHistoryReconstructionCfg{beaconCfg: &cfg, downloader: downloader, startingRoot: root, startingSlot: 10, indiciesDB: db, logger: log.New()}, t.Context(), log.New())
	require.ErrorIs(t, err, stop)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got, err := beacon_indicies.ReadCanonicalBlockRoot(tx, 10)
		require.NoError(t, err)
		require.Equal(t, common.Hash(root), got)
		executionHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, executionHash)
		return nil
	}))
}

func TestHistoryDownloadStoresOnlyVerifiableEnvelopeSignatures(t *testing.T) {
	for _, test := range []struct {
		name       string
		verifiable bool
	}{
		{name: "verifiable signature is stored", verifiable: true},
		{name: "unverifiable signature is not stored"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, bid, envelope, _ := validAnchorEnvelopeFixture(t, 1)
			block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
			block.Block.Slot = bid.Slot
			block.Block.Body.GetSignedExecutionPayloadBid().Message = bid
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)
			envelope.Message.BeaconBlockRoot = blockRoot

			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			dirs := datadir.New(t.TempDir())
			snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, cfg, dirs, log.New())
			t.Cleanup(snapshots.Close)
			stop := errors.New("stop after downloaded block")
			downloader := &oneBlockHistoryDownloader{
				failingHistoryDownloader: failingHistoryDownloader{err: stop, gloasEnvelopeSignatureVerifiable: test.verifiable},
				block:                    block,
				envelope:                 envelope,
			}

			err = SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
				beaconCfg:    cfg,
				downloader:   downloader,
				startingRoot: blockRoot,
				startingSlot: block.Block.Slot,
				indiciesDB:   db,
				sn:           snapshots,
				logger:       log.New(),
			}, t.Context(), log.New())
			require.ErrorIs(t, err, stop)
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				storedBlock, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot))
				require.NoError(t, err)
				require.NotEmpty(t, storedBlock)
				blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
				require.NoError(t, err)
				require.NotNil(t, blockNumber)
				require.Equal(t, envelope.Message.Payload.BlockNumber, *blockNumber)
				blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
				require.NoError(t, err)
				require.Equal(t, envelope.Message.Payload.BlockHash, blockHash)
				signature, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, blockRoot)
				require.NoError(t, err)
				require.Equal(t, test.verifiable, found)
				if found {
					require.Equal(t, envelope.Signature, signature)
				}
				return nil
			}))
		})
	}
}

func TestWriteDownloadedBlockStoresOnlyVerifiableSignaturesForSnapshottedBlock(t *testing.T) {
	for _, test := range []struct {
		name       string
		verifiable bool
	}{
		{name: "verifiable signature is stored", verifiable: true},
		{name: "unverifiable signature is not stored"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, bid, envelope, _ := validAnchorEnvelopeFixture(t, 1)
			block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
			block.Block.Slot = bid.Slot
			block.Block.Body.GetSignedExecutionPayloadBid().Message = bid
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)
			envelope.Message.BeaconBlockRoot = blockRoot
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return writeDownloadedBlock(tx, block, envelope, true, test.verifiable)
			}))
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				storedBlock, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot))
				require.NoError(t, err)
				require.Empty(t, storedBlock)
				signature, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, blockRoot)
				require.NoError(t, err)
				require.Equal(t, test.verifiable, found)
				if found {
					require.Equal(t, envelope.Signature, signature)
				}
				return nil
			}))
		})
	}
}

func TestWriteDownloadedBlockPreservesExistingEnvelopeSignature(t *testing.T) {
	for _, test := range []struct {
		name        string
		inSnapshots bool
	}{
		{name: "database block"},
		{name: "snapshot block", inSnapshots: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _, bid, envelope, _ := validAnchorEnvelopeFixture(t, 1)
			block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
			block.Block.Slot = bid.Slot
			block.Block.Body.GetSignedExecutionPayloadBid().Message = bid
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)
			envelope.Message.BeaconBlockRoot = blockRoot
			existingSignature := common.Bytes96{1}
			require.NotEqual(t, existingSignature, envelope.Signature)
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return beacon_indicies.WriteExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, blockRoot, existingSignature)
			}))
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return writeDownloadedBlock(tx, block, envelope, test.inSnapshots, true)
			}))
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				signature, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, blockRoot)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, existingSignature, signature)
				return nil
			}))
		})
	}
}

func TestWriteDownloadedBlockReplacesMalformedExistingEnvelopeSignature(t *testing.T) {
	cfg, _, bid, envelope, _ := validAnchorEnvelopeFixture(t, 1)
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = bid.Slot
	block.Block.Body.GetSignedExecutionPayloadBid().Message = bid
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = blockRoot
	malformedSignature := make([]byte, 95)
	malformedSignature[0] = 1
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.ExecutionPayloadEnvelopeSignatures, dbutils.BlockBodyKey(block.Block.Slot, blockRoot), malformedSignature)
	}))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return writeDownloadedBlock(tx, block, envelope, false, true)
	}))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		storedBlock, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot))
		require.NoError(t, err)
		require.NotEmpty(t, storedBlock)
		executionBlockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.NotNil(t, executionBlockNumber)
		require.Equal(t, envelope.Message.Payload.BlockNumber, *executionBlockNumber)
		executionHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, envelope.Message.Payload.BlockHash, executionHash)
		storedSignature, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, blockRoot)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, envelope.Signature, storedSignature)
		return nil
	}))
}

func TestHistoryDownloadEmptyGloasBlockStoresNoEnvelopeSignature(t *testing.T) {
	cfg, _, bid, _, _ := validAnchorEnvelopeFixture(t, 1)
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = bid.Slot
	block.Block.Body.GetSignedExecutionPayloadBid().Message = bid
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	stop := errors.New("stop after downloaded block")
	downloader := &oneBlockHistoryDownloader{
		failingHistoryDownloader: failingHistoryDownloader{err: stop},
		block:                    block,
	}

	err = SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:    cfg,
		downloader:   downloader,
		startingRoot: blockRoot,
		startingSlot: block.Block.Slot,
		indiciesDB:   db,
		sn:           snapshots,
		logger:       log.New(),
	}, t.Context(), log.New())
	require.ErrorIs(t, err, stop)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		_, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, blockRoot)
		require.NoError(t, err)
		require.False(t, found)
		return nil
	}))
}

func TestWaitForHistoryCompletion(t *testing.T) {
	t.Run("asynchronous caller returns immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.NoError(t, waitForHistoryCompletion(ctx, make(chan struct{}), false))
	})

	t.Run("synchronous caller waits for completion", func(t *testing.T) {
		finishCh := make(chan struct{})
		close(finishCh)

		require.NoError(t, waitForHistoryCompletion(context.Background(), finishCh, true))
	})

	t.Run("synchronous caller remains owned until cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- waitForHistoryCompletion(ctx, make(chan struct{}), true)
		}()
		cancel()

		require.ErrorIs(t, <-done, context.Canceled)
	})

	t.Run("cancellation wins when completion is also ready", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		finishCh := make(chan struct{})
		close(finishCh)

		require.ErrorIs(t, waitForHistoryCompletion(ctx, finishCh, true), context.Canceled)
	})
}
