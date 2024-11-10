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

package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/dataflow"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/bordb"
	"github.com/erigontech/erigon/polygon/bor/finality"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/sync"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

const (
	inmemorySnapshots       = 128  // Number of recent vote snapshots to keep in memory
	snapshotPersistInterval = 1024 // Number of blocks after which to persist the vote snapshot to the database
)

type BorHeimdallCfg struct {
	db              kv.RwDB
	snapDb          kv.RwDB // Database to store and retrieve snapshot checkpoints
	miningState     *MiningState
	chainConfig     *chain.Config
	borConfig       *borcfg.BorConfig
	heimdallClient  heimdall.HeimdallClient
	heimdallStore   heimdall.Store
	bridgeStore     bridge.Store
	blockReader     services.FullBlockReader
	hd              *headerdownload.HeaderDownload
	penalize        func(context.Context, []headerdownload.PenaltyItem)
	recents         *lru.ARCCache[libcommon.Hash, *bor.Snapshot]
	signatures      *lru.ARCCache[libcommon.Hash, libcommon.Address]
	recordWaypoints bool
	unwindCfg       bordb.HeimdallUnwindCfg
}

func StageBorHeimdallCfg(
	db kv.RwDB,
	snapDb kv.RwDB,
	miningState MiningState,
	chainConfig chain.Config,
	heimdallClient heimdall.HeimdallClient,
	heimdallStore heimdall.Store,
	bridgeStore bridge.Store,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	penalize func(context.Context, []headerdownload.PenaltyItem),
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	recordWaypoints bool,
	userUnwindTypeOverrides []string,
) BorHeimdallCfg {
	var borConfig *borcfg.BorConfig
	if chainConfig.Bor != nil {
		borConfig = chainConfig.Bor.(*borcfg.BorConfig)
	}

	unwindCfg := bordb.HeimdallUnwindCfg{} // unwind everything by default
	if len(userUnwindTypeOverrides) > 0 {
		unwindCfg.ApplyUserUnwindTypeOverrides(userUnwindTypeOverrides)
	}

	return BorHeimdallCfg{
		db:              db,
		snapDb:          snapDb,
		miningState:     &miningState,
		chainConfig:     &chainConfig,
		borConfig:       borConfig,
		heimdallClient:  heimdallClient,
		heimdallStore:   heimdallStore,
		bridgeStore:     bridgeStore,
		blockReader:     blockReader,
		hd:              hd,
		penalize:        penalize,
		recents:         recents,
		signatures:      signatures,
		recordWaypoints: recordWaypoints,
		unwindCfg:       unwindCfg,
	}
}

func BorHeimdallForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg BorHeimdallCfg,
	logger log.Logger,
) (err error) {
	processStart := time.Now()
	if cfg.borConfig == nil || cfg.heimdallClient == nil {
		return
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}

	whitelistService := whitelist.GetWhitelistingService()
	if unwindPointPtr := finality.BorMilestoneRewind.Load(); unwindPointPtr != nil && *unwindPointPtr != 0 {
		unwindPoint := *unwindPointPtr
		if whitelistService != nil && unwindPoint < headNumber {
			header, err := cfg.blockReader.HeaderByNumber(ctx, tx, headNumber)
			if err != nil {
				return err
			}

			hash := header.Hash()
			logger.Debug(
				fmt.Sprintf("[%s] Verification failed for header due to milestone rewind", s.LogPrefix()),
				"hash", hash,
				"height", headNumber,
			)
			cfg.penalize(ctx, []headerdownload.PenaltyItem{{
				Penalty: headerdownload.BadBlockPenalty,
				PeerID:  cfg.hd.SourcePeerId(hash),
			}})
			dataflow.HeaderDownloadStates.AddChange(headNumber, dataflow.HeaderInvalidated)
			if err := s.state.UnwindTo(unwindPoint, ForkReset(hash), tx); err != nil {
				return err
			}
			var reset uint64 = 0
			finality.BorMilestoneRewind.Store(&reset)
			return fmt.Errorf("verification failed for header %d: %x", headNumber, header.Hash())
		}
	}

	if s.BlockNumber == headNumber {
		return nil
	}

	lastBlockNum := s.BlockNumber
	if cfg.blockReader.FrozenBorBlocks() > lastBlockNum {
		lastBlockNum = cfg.blockReader.FrozenBorBlocks()
	}

	recents, err := lru.NewARC[libcommon.Hash, *bor.Snapshot](inmemorySnapshots)
	if err != nil {
		return err
	}

	signatures, err := lru.NewARC[libcommon.Hash, libcommon.Address](sync.InMemorySignatures)
	if err != nil {
		return err
	}

	var blockNum uint64
	var fetchTime time.Duration
	var snapTime time.Duration
	var snapInitTime time.Duration
	var syncEventTime time.Duration

	var eventRecords int

	lastSpanID, err := fetchRequiredHeimdallSpansIfNeeded(ctx, headNumber, tx, cfg, s.LogPrefix(), logger)
	if err != nil {
		return err
	}

	var lastCheckpointId, lastMilestoneId uint64

	var waypointTime time.Duration

	if cfg.recordWaypoints {
		waypointStart := time.Now()

		lastCheckpointId, err = fetchAndWriteHeimdallCheckpointsIfNeeded(ctx, headNumber, tx, cfg, s.LogPrefix(), logger)

		if err != nil {
			return err
		}

		lastMilestoneId, err = fetchAndWriteHeimdallMilestonesIfNeeded(ctx, headNumber, tx, cfg, s.LogPrefix(), logger)

		if err != nil {
			return err
		}

		waypointTime = waypointTime + time.Since(waypointStart)
	}

	lastStateSyncEventID, _, err := cfg.blockReader.LastEventId(ctx, tx)
	if err != nil {
		return err
	}

	chainReader := NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger)
	logTimer := time.NewTicker(logInterval)
	defer logTimer.Stop()

	logger.Info(fmt.Sprintf("[%s] Processing sync events...", s.LogPrefix()), "from", lastBlockNum+1, "to", headNumber)
	var nextEventRecord *heimdall.EventRecordWithTime

	// sometimes via config events are skipped from particular blocks and
	// pushed into the next one, when this happens we need to skip validation
	// as the times won't match the expected window. In practice it only affects
	// these blocks: 14949120,14949184, 14953472, 14953536, 14953600, 14953664,
	// 14953728, 14953792, 14953856 so it seems keeping a local skip marker is good
	// enough - it will only impact sync from origin operations.  If
	// this becomes more prevalent this will need to be re-thought
	var skipCount int

	// allow commit every N blocks to avoid long transactions and lost progress
	// N=1000 is not verified to be most optimal value, but works fine in unit tests
	var commitBatchLimit = 1_000
	var commitCnt int

	// newTx==true means a batch has been committed and should init a fresh new tx to handle next batch
	newTx := false
	for blockNum = lastBlockNum + 1; blockNum <= headNumber; blockNum++ {
		if !useExternalTx && newTx {
			newTx = false
			tx, err = cfg.db.BeginRw(ctx)
			if err != nil {
				return err
			}
			chainReader = NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger)
		}
		select {
		default:
		case <-logTimer.C:
			logger.Info(
				fmt.Sprintf("[%s] StateSync Progress", s.LogPrefix()),
				"progress", blockNum,
				"lastSpanID", lastSpanID,
				"lastCheckpointId", lastCheckpointId,
				"lastMilestoneId", lastMilestoneId,
				"lastStateSyncEventID", lastStateSyncEventID,
				"total records", eventRecords,
				"sync-events", syncEventTime,
				"sync-event-fetch", fetchTime,
				"snaps", snapTime,
				"snap-init", snapInitTime,
				"waypoints", waypointTime,
				"process time", time.Since(processStart),
			)
		}

		header, err := cfg.blockReader.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			_, _ = cfg.blockReader.HeaderByNumber(dbg.ContextWithDebug(ctx, true), tx, blockNum)
			return fmt.Errorf("header not found: %d", blockNum)
		}

		// Whitelist whitelistService is called to check if the bor chainReader is
		// on the canonical chainReader according to milestones
		if whitelistService != nil && !whitelistService.IsValidChain(blockNum, []*types.Header{header}) {
			logger.Debug(
				fmt.Sprintf("[%s] Verification failed for header", s.LogPrefix()),
				"height", blockNum,
				"hash", header.Hash(),
			)

			cfg.penalize(ctx, []headerdownload.PenaltyItem{{
				Penalty: headerdownload.BadBlockPenalty,
				PeerID:  cfg.hd.SourcePeerId(header.Hash()),
			}})

			dataflow.HeaderDownloadStates.AddChange(blockNum, dataflow.HeaderInvalidated)
			if err := s.state.UnwindTo(blockNum-1, ForkReset(header.Hash()), tx); err != nil {
				return err
			}
			return fmt.Errorf("verification failed for header %d: %x", blockNum, header.Hash())
		}

		snapStart := time.Now()

		if cfg.blockReader.BorSnapshots().SegmentsMin() == 0 {
			snapTime = snapTime + time.Since(snapStart)
			// SegmentsMin is only set if running as an uploader process (check SnapshotsCfg.snapshotUploader and
			// SegmentsMin is only set if running as an uploader process (check SnapshotsCfg.snapshotUploader and
			// UploadLocationFlag) when we remove snapshots based on FrozenBlockLimit and number of uploaded snapshots
			// avoid calling this if block for blockNums <= SegmentsMin to avoid reinsertion of snapshots
			snap := loadSnapshot(blockNum, header.Hash(), cfg.borConfig, recents, signatures, cfg.snapDb, logger)

			lastPersistedBlockNum, err := lastPersistedSnapshotBlock(cfg.snapDb)
			if err != nil {
				return err
			}

			// this will happen if for example chaindb is removed
			if lastPersistedBlockNum > blockNum {
				lastPersistedBlockNum = blockNum
			}

			// if the last time we persisted snapshots is too far away re-run the forward
			// initialization process - this is to avoid memory growth due to recusrion
			// in persistValidatorSets
			if snap == nil && (blockNum == 1 || blockNum-lastPersistedBlockNum > (snapshotPersistInterval*5)) {
				snap, err = initValidatorSets(
					ctx,
					tx,
					cfg.blockReader,
					cfg.borConfig,
					cfg.heimdallClient,
					cfg.heimdallStore,
					chainReader,
					blockNum,
					lastPersistedBlockNum,
					recents,
					signatures,
					cfg.snapDb,
					logger,
					s.LogPrefix(),
				)
				if err != nil {
					return fmt.Errorf("can't initialise validator sets: %w", err)
				}
			}

			snapInitTime = snapInitTime + time.Since(snapStart)

			err = persistValidatorSets(
				snap,
				u,
				tx,
				cfg.borConfig,
				chainReader,
				blockNum,
				header.Hash(),
				recents,
				signatures,
				cfg.snapDb,
				logger,
				s.LogPrefix(),
			)
			if err != nil {
				return fmt.Errorf("can't persist validator sets: %w", err)
			}
		}

		snapTime = snapTime + time.Since(snapStart)

		if err := checkBorHeaderExtraDataIfRequired(chainReader, header, cfg.borConfig); err != nil {
			return err
		}

		snapTime = snapTime + time.Since(snapStart)

		syncEventStart := time.Now()
		var callTime time.Duration

		var endStateSyncEventId uint64

		if nextEventRecord == nil || header.Time > uint64(nextEventRecord.Time.Unix()) {
			var records int

			if lastStateSyncEventID == 0 || lastStateSyncEventID != endStateSyncEventId {
				lastStateSyncEventID, records, skipCount, callTime, err = fetchRequiredHeimdallStateSyncEventsIfNeeded(
					ctx,
					header,
					tx,
					cfg.borConfig,
					cfg.blockReader,
					cfg.heimdallClient,
					cfg.bridgeStore,
					cfg.chainConfig.ChainID.String(),
					s.LogPrefix(),
					logger,
					lastStateSyncEventID,
					skipCount,
				)

				if err != nil {
					return err
				}
			}

			if records != 0 {
				nextEventRecord = nil
				eventRecords += records
			} else {
				if nextEventRecord == nil || nextEventRecord.ID <= lastStateSyncEventID {
					if eventRecord, err := cfg.heimdallClient.FetchStateSyncEvent(ctx, lastStateSyncEventID+1); err == nil {
						nextEventRecord = eventRecord
						endStateSyncEventId = 0
					} else {
						if !errors.Is(err, heimdall.ErrEventRecordNotFound) {
							return err
						}
						endStateSyncEventId = lastStateSyncEventID
					}
				}
			}
		}

		fetchTime += callTime
		syncEventTime = syncEventTime + time.Since(syncEventStart)

		commitCnt++
		if (commitCnt >= commitBatchLimit && !useExternalTx) || blockNum == headNumber {
			newTx = true
			if err = s.Update(tx, blockNum); err != nil {
				return err
			}
			lastStateSyncEventID, _, _ = cfg.blockReader.LastEventId(ctx, tx)

			if err = tx.Commit(); err != nil {
				return err
			}

			logger.Info(
				fmt.Sprintf("[%s] Sync events", s.LogPrefix()),
				"progress", blockNum-1,
				"lastSpanID", lastSpanID,
				"lastSpanID", lastSpanID,
				"lastCheckpointId", lastCheckpointId,
				"lastMilestoneId", lastMilestoneId,
				"lastStateSyncEventID", lastStateSyncEventID,
				"total records", eventRecords,
				"sync event time", syncEventTime,
				"fetch time", fetchTime,
				"snap time", snapTime,
				"waypoint time", waypointTime,
				"process time", time.Since(processStart),
			)

			// reset metrics
			commitCnt = 0
			eventRecords = 0
			syncEventTime = time.Duration(0)
			fetchTime = time.Duration(0)
			snapTime = time.Duration(0)
			waypointTime = time.Duration(0)
			processStart = time.Now()
		}
	}
	return
}

func loadSnapshot(
	blockNum uint64,
	hash libcommon.Hash,
	config *borcfg.BorConfig,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	snapDb kv.RwDB,
	logger log.Logger,
) *bor.Snapshot {

	if s, ok := recents.Get(hash); ok {
		return s
	}

	if blockNum%snapshotPersistInterval == 0 {
		if s, err := bor.LoadSnapshot(config, signatures, snapDb, hash); err == nil {
			logger.Trace("Loaded snapshot from disk", "number", blockNum, "hash", hash)
			return s
		}
	}

	return nil
}

func persistValidatorSets(
	snap *bor.Snapshot,
	u Unwinder,
	chainDBTx kv.Tx,
	config *borcfg.BorConfig,
	chain consensus.ChainHeaderReader,
	blockNum uint64,
	hash libcommon.Hash,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	snapDb kv.RwDB,
	logger log.Logger,
	logPrefix string,
) error {

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	// Search for a snapshot in memory or on disk for checkpoints

	headers := make([]*types.Header, 0, 16)
	var parent *types.Header

	if s, ok := recents.Get(hash); ok {
		snap = s
	}

	count := 0
	dbsize := uint64(0)

	//nolint:govet
	for snap == nil {
		// If an on-disk snapshot can be found, use that
		if blockNum%snapshotPersistInterval == 0 {
			if s, err := bor.LoadSnapshot(config, signatures, snapDb, hash); err == nil {
				logger.Trace("Loaded snapshot from disk", "number", blockNum, "hash", hash)

				snap = s

				break
			}
		}

		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		// No explicit parents (or no more left), reach out to the database
		if parent != nil {
			header = parent
		} else if chain != nil {
			header = chain.GetHeader(hash, blockNum)
			//logger.Info(fmt.Sprintf("header %d %x => %+v\n", header.Number.Uint64(), header.Hash(), header))
		}

		if header == nil {
			return consensus.ErrUnknownAncestor
		}

		if blockNum == 0 {
			break
		}

		headers = append(headers, header)
		blockNum, hash = blockNum-1, header.ParentHash
		if chain != nil {
			parent = chain.GetHeader(hash, blockNum)
		}

		// If an in-memory snapshot was found, use that
		if s, ok := recents.Get(hash); ok {
			snap = s
			break
		}

		select {
		case <-logEvery.C:
			if dbsize == 0 {
				_ = snapDb.View(context.Background(), func(tx kv.Tx) error {
					dbsize, _ = tx.Count(kv.BorSeparate)
					return nil
				})
			}
			logger.Info(
				fmt.Sprintf("[%s] Gathering headers for validator proposer prorities (backwards)", logPrefix),
				"processed", count, "blockNum", blockNum, "dbsize", dbsize,
			)
		default:
		}

		count++
	}

	// check if snapshot is nil
	if snap == nil {
		return fmt.Errorf("unknown error while retrieving snapshot at block number %v", blockNum)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	if len(headers) > 0 {
		var err error
		if snap, err = snap.Apply(parent, headers, logger); err != nil {
			if snap != nil {
				var badHash libcommon.Hash
				for _, header := range headers {
					if header.Number.Uint64() == snap.Number+1 {
						badHash = header.Hash()
						break
					}
				}
				if err := u.UnwindTo(snap.Number, BadBlock(badHash, err), chainDBTx); err != nil {
					return err
				}
			} else {
				return fmt.Errorf(
					"snap.Apply %d, headers %d-%d: %w",
					blockNum,
					headers[0].Number.Uint64(),
					headers[len(headers)-1].Number.Uint64(),
					err,
				)
			}
		}
	}

	recents.Add(snap.Hash, snap)

	// If we've generated a new persistent snapshot, save to disk
	if snap.Number%snapshotPersistInterval == 0 && len(headers) > 0 {
		if err := snap.Store(snapDb); err != nil {
			return fmt.Errorf("snap.Store: %w", err)
		}

		logger.Debug(
			fmt.Sprintf("[%s] Stored proposer snapshot to disk (persist)", logPrefix),
			"number", snap.Number,
			"hash", snap.Hash,
		)
	}

	return nil
}

func lastPersistedSnapshotBlock(snapDb kv.RwDB) (uint64, error) {
	var lastPersistedBlockNum uint64

	err := snapDb.View(context.Background(), func(tx kv.Tx) error {
		progressBytes, err := tx.GetOne(kv.BorSeparate, []byte("bor-snapshot-progress"))
		if err != nil {
			return err
		}

		if len(progressBytes) == 8 {
			lastPersistedBlockNum = binary.BigEndian.Uint64(progressBytes)
		}

		return nil
	})

	return lastPersistedBlockNum, err
}

func initValidatorSets(
	ctx context.Context,
	tx kv.RwTx,
	blockReader services.FullBlockReader,
	config *borcfg.BorConfig,
	heimdallClient heimdall.HeimdallClient,
	heimdallStore heimdall.Store,
	chain consensus.ChainHeaderReader,
	blockNum uint64,
	lastPersistedBlockNum uint64,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	snapDb kv.RwDB,
	logger log.Logger,
	logPrefix string,
) (*bor.Snapshot, error) {

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	var snap *bor.Snapshot

	var parentHeader *types.Header
	var firstBlockNum uint64

	if lastPersistedBlockNum > 0 {
		parentHeader = chain.GetHeaderByNumber(lastPersistedBlockNum)
		if parentHeader == nil {
			return nil, fmt.Errorf("[%s] header not found: %d", logPrefix, lastPersistedBlockNum)
		}
		snap = loadSnapshot(lastPersistedBlockNum, parentHeader.Hash(), config, recents, signatures, snapDb, logger)
		firstBlockNum = lastPersistedBlockNum + 1
	} else {
		// Special handling of the headers in the snapshot
		zeroHeader := chain.GetHeaderByNumber(0)

		if zeroHeader != nil {
			// get checkpoint data
			hash := zeroHeader.Hash()

			if snap = loadSnapshot(0, hash, config, recents, signatures, snapDb, logger); snap == nil {
				// get validators and current span
				zeroSpan, _, err := blockReader.Span(ctx, tx, 0)

				if err != nil {
					if _, err := fetchAndWriteHeimdallSpan(ctx, 0, tx, heimdallClient, heimdallStore, logPrefix, logger); err != nil {
						return nil, err
					}

					zeroSpan, _, err = blockReader.Span(ctx, tx, 0)

					if err != nil {
						return nil, err
					}
				}

				if zeroSpan == nil {
					return nil, errors.New("zero span not found")
				}

				// new snap shot
				snap = bor.NewSnapshot(config, signatures, 0, hash, zeroSpan.ValidatorSet.Validators, logger)
				if err := snap.Store(snapDb); err != nil {
					return nil, fmt.Errorf("snap.Store (0): %w", err)
				}

				logger.Debug(fmt.Sprintf("[%s] Stored proposer snapshot to disk (init)", logPrefix), "number", 0, "hash", hash)
			}

			parentHeader = zeroHeader
			firstBlockNum = 1
			lastPersistedBlockNum = 0
		}
	}

	g := errgroup.Group{}
	g.SetLimit(estimate.AlmostAllCPUs())
	defer func() {
		_ = g.Wait() // goroutines used in this err group do not return err (check below)
	}()

	batchSize := 128 // must be < InMemorySignatures
	initialHeaders := make([]*types.Header, 0, batchSize)

	var err error

	for i := firstBlockNum; i <= blockNum; i++ {
		header := chain.GetHeaderByNumber(i)
		{
			// `snap.apply` bottleneck - is recover of signer.
			// to speedup: recover signer in background goroutines and save in `sigcache`
			// `batchSize` < `InMemorySignatures`: means all current batch will fit in cache - and
			// `snap.apply` will find it there.
			g.Go(func() error {
				if header == nil {
					return nil
				}
				_, _ = bor.Ecrecover(header, signatures, config)
				return nil
			})
		}
		if header == nil {
			return nil, fmt.Errorf("missing header persisting validator sets: (inside loop at %d)", i)
		}
		initialHeaders = append(initialHeaders, header)

		if len(initialHeaders) == cap(initialHeaders) {

			if snap, err = snap.Apply(parentHeader, initialHeaders, logger); err != nil {
				return nil, fmt.Errorf("snap.Apply (inside loop): %w", err)
			}

			parentHeader = initialHeaders[len(initialHeaders)-1]
			initialHeaders = initialHeaders[:0]

			// If we've generated a new persistent snapshot, save to disk
			if snap.Number%snapshotPersistInterval == 0 {
				if err := snap.Store(snapDb); err != nil {
					return nil, fmt.Errorf("snap.Store: %w", err)
				}

				lastPersistedBlockNum = snap.Number

				logger.Trace(
					fmt.Sprintf("[%s] Stored proposer snapshot to disk (init loop)", logPrefix),
					"number", snap.Number,
					"hash", snap.Hash,
				)
			}
		}

		select {
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[%s] Computing validator proposer prorities (forward)", logPrefix), "to", blockNum, "snapNum", i, "persisted", lastPersistedBlockNum)
		default:
		}
	}

	if snap, err = snap.Apply(parentHeader, initialHeaders, logger); err != nil {
		return nil, fmt.Errorf("snap.Apply (outside loop): %w", err)
	}

	return snap, nil
}

func checkBorHeaderExtraDataIfRequired(chr chainHeaderReader, header *types.Header, cfg *borcfg.BorConfig) error {
	if !cfg.IsSprintEnd(header.Number.Uint64()) {
		// not last block of a sprint in a span, so no check needed (we only check last block of a sprint)
		return nil
	}

	return checkBorHeaderExtraData(chr, header, cfg)
}

type chainHeaderReader interface {
	// bor span with given ID
	BorSpan(spanId uint64) *heimdall.Span
}

func checkBorHeaderExtraData(chr chainHeaderReader, header *types.Header, cfg *borcfg.BorConfig) error {
	spanID := heimdall.SpanIdAt(header.Number.Uint64() + 1)
	sp := chr.BorSpan(uint64(spanID))

	producerSet := make([]*valset.Validator, len(sp.SelectedProducers))
	for i := range sp.SelectedProducers {
		producerSet[i] = &sp.SelectedProducers[i]
	}

	sort.Sort(valset.ValidatorsByAddress(producerSet))

	headerVals, err := valset.ParseValidators(bor.GetValidatorBytes(header, cfg))
	if err != nil {
		return err
	}

	// span 0 at least for mumbai has a header mismatch in
	// its first spam.  Since we control neither the span, not the
	// the headers (they are external data) - we just don't do the
	// check as it will hault further processing
	if len(producerSet) != len(headerVals) && spanID > 0 {
		return ErrHeaderValidatorsLengthMismatch
	}

	for i, val := range producerSet {
		if !bytes.Equal(val.HeaderBytes(), headerVals[i].HeaderBytes()) {
			return ErrHeaderValidatorsBytesMismatch
		}
	}

	return nil
}

func BorHeimdallUnwind(u *UnwindState, ctx context.Context, _ *StageState, tx kv.RwTx, cfg BorHeimdallCfg) (err error) {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBorBlocks()) // protect from unwind behind files

	if cfg.borConfig == nil {
		return
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = bordb.UnwindHeimdall(ctx, cfg.heimdallStore, cfg.bridgeStore, tx, u.UnwindPoint, cfg.unwindCfg); err != nil {
		return err
	}

	if err = u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return
}
