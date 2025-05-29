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
	"context"
	"errors"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/types/accounts"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/prune"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/silkworm"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

const (
	logInterval = 30 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000
)

type headerDownloader interface {
	ReportBadHeaderPoS(badHeader, lastValidAncestor common.Hash)
	POSSync() bool
}

type ExecuteBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	chainConfig   *chain.Config
	notifications *shards.Notifications
	engine        consensus.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	blockReader   services.FullBlockReader
	hd            headerDownloader
	author        *common.Address
	// last valid number of the stage

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis

	silkworm        *silkworm.Silkworm
	blockProduction bool

	applyWorker, applyWorkerMining *exec3.Worker
}

func StageExecuteBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.Config,
	notifications *shards.Notifications,
	stateStream bool,
	badBlockHalt bool,

	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	silkworm *silkworm.Silkworm,
) ExecuteBlockCfg {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}

	return ExecuteBlockCfg{
		db:                db,
		prune:             pm,
		batchSize:         batchSize,
		chainConfig:       chainConfig,
		engine:            engine,
		vmConfig:          vmConfig,
		dirs:              dirs,
		notifications:     notifications,
		stateStream:       stateStream,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,
		genesis:           genesis,
		historyV3:         true,
		syncCfg:           syncCfg,
		silkworm:          silkworm,
		applyWorker:       exec3.NewWorker(nil, log.Root(), vmConfig.Tracer, context.Background(), false, db, nil, blockReader, chainConfig, genesis, nil, engine, dirs, false),
		applyWorkerMining: exec3.NewWorker(nil, log.Root(), vmConfig.Tracer, context.Background(), false, db, nil, blockReader, chainConfig, genesis, nil, engine, dirs, true),
	}
}

// ================ Erigon3 ================

func ExecBlockV3(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger, isMining bool) (err error) {
	workersCount := cfg.syncCfg.ExecWorkerCount
	if !initialCycle {
		workersCount = 1
	}

	prevStageProgress, err := stageProgress(txc.Tx, cfg.db, stages.Senders)
	if err != nil {
		return err
	}

	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to < s.BlockNumber {
		return nil
	}

	parallel := txc.Tx == nil
	if err := ExecV3(ctx, s, u, workersCount, cfg, txc, parallel, to, logger, cfg.vmConfig.Tracer, initialCycle, isMining); err != nil {
		return err
	}
	return nil
}

var ErrTooDeepUnwind = errors.New("too deep unwind")

func unwindExec3(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	br := cfg.blockReader
	var domains *libstate.SharedDomains
	var tx kv.TemporalRwTx
	if txc.Doms == nil {
		temporalTx, ok := txc.Tx.(kv.TemporalRwTx)
		if !ok {
			return errors.New("tx is not a temporal tx")
		}
		tx = temporalTx
		domains, err = libstate.NewSharedDomains(temporalTx, logger)
		if err != nil {
			return err
		}
		defer domains.Close()
	} else {
		tx = txc.Ttx.(kv.TemporalRwTx)
		domains = txc.Doms
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, br))

	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	txNum, err := txNumsReader.Min(tx, u.UnwindPoint+1)
	if err != nil {
		return err
	}

	t := time.Now()
	var changeset *[kv.DomainLen][]kv.DomainEntryDiff
	for currentBlock := u.CurrentBlockNumber; currentBlock > u.UnwindPoint; currentBlock-- {
		currentHash, ok, err := br.CanonicalHash(ctx, tx, currentBlock)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("canonical hash not found %d", currentBlock)
		}
		var currentKeys [kv.DomainLen][]kv.DomainEntryDiff
		currentKeys, ok, err = domains.GetDiffset(tx, currentHash, currentBlock)
		if !ok {
			return fmt.Errorf("domains.GetDiffset(%d, %s): not found", currentBlock, currentHash)
		}
		if err != nil {
			return err
		}
		if changeset == nil {
			changeset = &currentKeys
		} else {
			for i := range currentKeys {
				changeset[i] = libstate.MergeDiffSets(changeset[i], currentKeys[i])
			}
		}
	}
	if err := unwindExec3State(ctx, tx, domains, u.UnwindPoint, txNum, accumulator, changeset, logger); err != nil {
		return fmt.Errorf("ParallelExecutionState.Unwind(%d->%d): %w, took %s", s.BlockNumber, u.UnwindPoint, err, time.Since(t))
	}
	if err := rawdb.DeleteNewerEpochs(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}
	return nil
}

var (
	mxState3UnwindRunning = metrics.GetOrCreateGauge("state3_unwind_running")
	mxState3Unwind        = metrics.GetOrCreateSummary("state3_unwind")
)

func unwindExec3State(ctx context.Context, tx kv.TemporalRwTx, sd *libstate.SharedDomains,
	blockUnwindTo, txUnwindTo uint64,
	accumulator *shards.Accumulator,
	changeset *[kv.DomainLen][]kv.DomainEntryDiff, logger log.Logger) error {
	mxState3UnwindRunning.Inc()
	defer mxState3UnwindRunning.Dec()
	st := time.Now()
	defer mxState3Unwind.ObserveDuration(st)
	var currentInc uint64

	//TODO: why we don't call accumulator.ChangeCode???
	handle := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == length.Addr {
			if len(v) > 0 {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, v); err != nil {
					return fmt.Errorf("%w, %x", err, v)
				}
				var address common.Address
				copy(address[:], k)

				newV := accounts.SerialiseV3(&acc)
				if accumulator != nil {
					accumulator.ChangeAccount(address, acc.Incarnation, newV)
				}
			} else {
				var address common.Address
				copy(address[:], k)
				if accumulator != nil {
					accumulator.DeleteAccount(address)
				}
			}
			return nil
		}

		var address common.Address
		var location common.Hash
		copy(address[:], k[:length.Addr])
		copy(location[:], k[length.Addr:])
		if accumulator != nil {
			accumulator.ChangeStorage(address, currentInc, location, common.Copy(v))
		}
		return nil
	}

	stateChanges := etl.NewCollector("", "", etl.NewOldestEntryBuffer(etl.BufferOptimalSize), logger)
	defer stateChanges.Close()
	stateChanges.SortAndFlushInBackground(true)

	accountDiffs := changeset[kv.AccountsDomain]
	for _, kv := range accountDiffs {
		if err := stateChanges.Collect(toBytesZeroCopy(kv.Key)[:length.Addr], kv.Value); err != nil {
			return err
		}
	}
	storageDiffs := changeset[kv.StorageDomain]
	for _, kv := range storageDiffs {
		if err := stateChanges.Collect(toBytesZeroCopy(kv.Key), kv.Value); err != nil {
			return err
		}
	}

	if err := stateChanges.Load(tx, "", handle, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := sd.Unwind(ctx, tx, blockUnwindTo, txUnwindTo, changeset); err != nil {
		return err
	}

	//sd.logger.Info("aggregator unwind", "txUnwindTo", txUnwindTo)
	//sf := time.Now()
	//defer mxUnwindSharedTook.ObserveDuration(sf)
	if err := sd.Flush(ctx, tx); err != nil {
		return err
	}

	if err := tx.Unwind(ctx, txUnwindTo, changeset); err != nil {
		return err
	}

	sd.ClearRam(true)
	sd.SetTxNum(txUnwindTo)
	sd.SetBlockNum(blockUnwindTo)
	return sd.Flush(ctx, tx)
}

func toBytesZeroCopy(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

func stageProgress(tx kv.Tx, db kv.RoDB, stage stages.SyncStage) (prevStageProgress uint64, err error) {
	if tx != nil {
		prevStageProgress, err = stages.GetStageProgress(tx, stage)
		if err != nil {
			return prevStageProgress, err
		}
	} else {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
			prevStageProgress, err = stages.GetStageProgress(tx, stage)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return prevStageProgress, err
		}
	}
	return prevStageProgress, nil
}

// ================ Erigon3 End ================

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	if dbg.StagesOnlyBlocks {
		return nil
	}
	if err = ExecBlockV3(s, u, txc, toBlock, ctx, cfg, s.CurrentSyncCycle.IsInitialCycle, logger, false); err != nil {
		return err
	}
	return nil
}

func blocksReadAhead(ctx context.Context, cfg *ExecuteBlockCfg, workers int, histV3 bool) (chan uint64, context.CancelFunc) {
	const readAheadBlocks = 100
	readAhead := make(chan uint64, readAheadBlocks)
	g, gCtx := errgroup.WithContext(ctx)
	for workerNum := 0; workerNum < workers; workerNum++ {
		g.Go(func() (err error) {
			var bn uint64
			var ok bool
			var tx kv.Tx
			defer func() {
				if tx != nil {
					tx.Rollback()
				}
			}()

			for i := 0; ; i++ {
				select {
				case bn, ok = <-readAhead:
					if !ok {
						return
					}
				case <-gCtx.Done():
					return gCtx.Err()
				}

				if i%100 == 0 {
					if tx != nil {
						tx.Rollback()
					}
					tx, err = cfg.db.BeginRo(ctx)
					if err != nil {
						return err
					}
				}

				if err := blocksReadAheadFunc(gCtx, tx, cfg, bn+readAheadBlocks, histV3); err != nil {
					return err
				}
			}
		})
	}
	return readAhead, func() {
		close(readAhead)
		_ = g.Wait()
	}
}
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, cfg *ExecuteBlockCfg, blockNum uint64, histV3 bool) error {
	block, err := cfg.blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	_, _ = cfg.engine.Author(block.HeaderNoCopy()) // Bor consensus: this calc is heavy and has cache

	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil
	}

	stateReader := state.NewReaderV3(ttx)
	senders := block.Body().SendersFromTxs()

	for _, sender := range senders {
		a, _ := stateReader.ReadAccountData(sender)
		if a == nil {
			continue
		}

		//Code domain using .bt index - means no false-positives
		if code, _ := stateReader.ReadAccountCode(sender); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}

	for _, txn := range block.Transactions() {
		to := txn.GetTo()
		if to != nil {
			a, _ := stateReader.ReadAccountData(*to)
			if a == nil {
				continue
			}
			//if account != nil && !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
			//	reader.Code(*tx.To(), common.BytesToHash(account.CodeHash))
			//}
			if code, _ := stateReader.ReadAccountCode(*to); len(code) > 0 {
				_, _ = code[0], code[len(code)-1]
			}

			for _, list := range txn.GetAccessList() {
				stateReader.ReadAccountData(list.Address)
				if len(list.StorageKeys) > 0 {
					for _, slot := range list.StorageKeys {
						stateReader.ReadAccountStorage(list.Address, slot)
					}
				}
			}
			//TODO: exec txn and pre-fetch commitment keys. see also: `func (p *statePrefetcher) Prefetch` in geth
		}

	}
	_, _ = stateReader.ReadAccountData(block.Coinbase())

	return nil
}

func UnwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	//fmt.Printf("unwind: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		txc.Tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer txc.Tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	logger.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	unwindToLimit, ok, err := libstate.AggTx(txc.Tx).CanUnwindBeforeBlockNum(u.UnwindPoint, txc.Tx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %d < %d", ErrTooDeepUnwind, u.UnwindPoint, unwindToLimit)
	}

	if err = unwindExecutionStage(u, s, txc, ctx, cfg, logger); err != nil {
		return err
	}
	if err = u.Done(txc.Tx); err != nil {
		return err
	}
	//dumpPlainStateDebug(tx, nil)

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) error {
	var accumulator *shards.Accumulator
	if cfg.stateStream && s.BlockNumber-u.UnwindPoint < stateStreamLimit {
		accumulator = cfg.notifications.Accumulator

		hash, ok, err := cfg.blockReader.CanonicalHash(ctx, txc.Tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		if !ok {
			return fmt.Errorf("canonical hash not found %d", u.UnwindPoint)
		}
		header, err := cfg.blockReader.HeaderByHash(ctx, txc.Tx, hash)
		if err != nil {
			return fmt.Errorf("read canonical header of unwind point: %w", err)
		}
		if header == nil {
			return fmt.Errorf("canonical header for unwind point not found: %s", hash)
		}
		txs, err := cfg.blockReader.RawTransactions(ctx, txc.Tx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(header, txs, true)
	}

	return unwindExec3(u, s, txc, ctx, cfg, accumulator, logger)
}

func PruneExecutionStage(s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if s.ForwardProgress > uint64(dbg.MaxReorgDepth) && !cfg.syncCfg.AlwaysGenerateChangesets {
		// (chunkLen is 8Kb) * (1_000 chunks) = 8mb
		// Some blocks on bor-mainnet have 400 chunks of diff = 3mb
		var pruneDiffsLimitOnChainTip = 1_000
		pruneTimeout := 250 * time.Millisecond
		if s.CurrentSyncCycle.IsInitialCycle {
			pruneDiffsLimitOnChainTip = math.MaxInt
			pruneTimeout = time.Hour
		}
		if err := rawdb.PruneTable(
			tx,
			kv.ChangeSets3,
			s.ForwardProgress-uint64(dbg.MaxReorgDepth),
			ctx,
			pruneDiffsLimitOnChainTip,
			pruneTimeout,
			logger,
			s.LogPrefix(),
		); err != nil {
			return err
		}
	}

	mxExecStepsInDB.Set(rawdbhelpers.IdxStepsCountV3(tx) * 100)

	// on chain-tip:
	//  - can prune only between blocks (without blocking blocks processing)
	//  - need also leave some time to prune blocks
	//  - need keep "fsync" time of db fast
	// Means - the best is:
	//  - stop prune when `tx.SpaceDirty()` is big
	//  - and set ~500ms timeout
	// because on slow disks - prune is slower. but for now - let's tune for nvme first, and add `tx.SpaceDirty()` check later https://github.com/erigontech/erigon/issues/11635
	pruneTimeout := 250 * time.Millisecond
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = 12 * time.Hour

		// allow greedy prune on non-chain-tip
		if err = tx.(kv.TemporalRwTx).GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
			return err
		}
	}

	if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, pruneTimeout); err != nil {
		return err
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
