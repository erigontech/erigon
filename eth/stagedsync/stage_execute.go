package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/prune"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

var stageExecutionGauge = metrics.NewRegisteredGauge("stage/execution", nil)

const (
	logInterval = 30 * time.Second
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type ExecuteBlockCfg struct {
	db            ethdb.RwKV
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook ChangeSetHook
	chainConfig   *params.ChainConfig
	engine        consensus.Engine
	vmConfig      *vm.Config
	tmpdir        string
	stateStream   bool
	accumulator   *shards.Accumulator
}

func StageExecuteBlocksCfg(
	kv ethdb.RwKV,
	prune prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook ChangeSetHook,
	chainConfig *params.ChainConfig,
	engine consensus.Engine,
	vmConfig *vm.Config,
	accumulator *shards.Accumulator,
	stateStream bool,
	tmpdir string,
) ExecuteBlockCfg {
	return ExecuteBlockCfg{
		db:            kv,
		prune:         prune,
		batchSize:     batchSize,
		changeSetHook: changeSetHook,
		chainConfig:   chainConfig,
		engine:        engine,
		vmConfig:      vmConfig,
		tmpdir:        tmpdir,
		accumulator:   accumulator,
		stateStream:   stateStream,
	}
}

func readBlock(blockNum uint64, tx ethdb.Tx) (*types.Block, error) {
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, _, err := rawdb.ReadBlockWithSenders(tx, blockHash, blockNum)
	return b, err
}

func executeBlock(
	block *types.Block,
	tx ethdb.RwTx,
	batch ethdb.Database,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	checkTEVM func(contractHash common.Hash) (bool, error),
	initialCycle bool,
) error {
	blockNum := block.NumberU64()
	stateReader, stateWriter := newStateReaderWriter(batch, tx, blockNum, block.Hash(), writeChangesets, cfg.accumulator, initialCycle, cfg.stateStream)

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }

	callTracer := NewCallTracer(checkTEVM)
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer
	receipts, err := core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHeader, cfg.engine, block, stateReader, stateWriter, epochReader{tx: tx}, chainReader{config: cfg.chainConfig, tx: tx}, checkTEVM)
	if err != nil {
		return err
	}

	if writeReceipts {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}
	}

	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}

	if writeCallTraces {
		callTracer.tos[block.Coinbase()] = false
		for _, uncle := range block.Uncles() {
			callTracer.tos[uncle.Coinbase] = false
		}
		list := make(common.Addresses, len(callTracer.froms)+len(callTracer.tos))
		i := 0
		for addr := range callTracer.froms {
			copy(list[i][:], addr[:])
			i++
		}
		for addr := range callTracer.tos {
			copy(list[i][:], addr[:])
			i++
		}
		sort.Sort(list)
		// List may contain duplicates
		var blockNumEnc [8]byte
		binary.BigEndian.PutUint64(blockNumEnc[:], blockNum)
		var prev common.Address
		var created bool
		for j, addr := range list {
			if j > 0 && prev == addr {
				continue
			}
			var v [common.AddressLength + 1]byte
			copy(v[:], addr[:])
			if _, ok := callTracer.froms[addr]; ok {
				v[common.AddressLength] |= 1
			}
			if _, ok := callTracer.tos[addr]; ok {
				v[common.AddressLength] |= 2
			}
			// TEVM marking still untranslated contracts
			if vmConfig.EnableTEMV {
				if created = callTracer.tos[addr]; created {
					v[common.AddressLength] |= 4
				}
			}
			if j == 0 {
				if err = tx.Append(dbutils.CallTraceSet, blockNumEnc[:], v[:]); err != nil {
					return err
				}
			} else {
				if err = tx.AppendDup(dbutils.CallTraceSet, blockNumEnc[:], v[:]); err != nil {
					return err
				}
			}
			copy(prev[:], addr[:])
		}
	}

	return nil
}

func newStateReaderWriter(
	batch ethdb.Database,
	tx ethdb.RwTx,
	blockNum uint64,
	blockHash common.Hash,
	writeChangesets bool,
	accumulator *shards.Accumulator,
	initialCycle bool,
	stateStream bool,
) (state.StateReader, state.WriterWithChangeSets) {

	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	stateReader = state.NewPlainStateReader(batch)

	if !initialCycle && stateStream {
		accumulator.StartChange(blockNum, blockHash, false)
	} else {
		accumulator = nil
	}
	if writeChangesets {
		stateWriter = state.NewPlainStateWriter(batch, tx, blockNum).SetAccumulator(accumulator)
	} else {
		stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(accumulator)
	}

	return stateReader, stateWriter
}

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, tx ethdb.RwTx, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	prevStageProgress, errStart := stages.GetStageProgress(tx, stages.Senders)
	if errStart != nil {
		return errStart
	}
	nextStageProgress, err := stages.GetStageProgress(tx, stages.HashState)
	if err != nil {
		return err
	}
	nextStagesExpectData := nextStageProgress > 0 // Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet

	logPrefix := s.LogPrefix()
	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to <= s.BlockNumber {
		return nil
	}
	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}

	var batch ethdb.DbWithPendingMutations
	batch = kv.NewBatch(tx, quit)
	defer batch.Rollback()

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress
	logTx, lastLogTx := uint64(0), uint64(0)
	logTime := time.Now()
	var gas uint64

	var stoppedErr error
Loop:
	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			break
		}
		var err error
		var block *types.Block
		if block, err = readBlock(blockNum, tx); err != nil {
			return err
		}
		if block == nil {
			log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			break
		}

		if err = cfg.engine.VerifyFamily(&chainReader{config: cfg.chainConfig, tx: tx}, block.Header()); err != nil {
			return err
		}

		lastLogTx += uint64(block.Transactions().Len())

		var checkTEVMCode func(contractHash common.Hash) (bool, error)

		if cfg.vmConfig.EnableTEMV {
			checkTEVMCode = ethdb.GetCheckTEVM(tx)
		}

		// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
		writeChangeSets := nextStagesExpectData || blockNum > cfg.prune.History.PruneTo(to)
		writeReceipts := nextStagesExpectData || blockNum > cfg.prune.Receipts.PruneTo(to)
		writeCallTraces := nextStagesExpectData || blockNum > cfg.prune.CallTraces.PruneTo(to)
		if err = executeBlock(block, tx, batch, cfg, *cfg.vmConfig, writeChangeSets, writeReceipts, writeCallTraces, checkTEVMCode, initialCycle); err != nil {
			log.Error(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "error", err)
			u.UnwindTo(blockNum-1, block.Hash())
			break Loop
		}
		stageProgress = blockNum

		updateProgress := batch.BatchSize() >= int(cfg.batchSize)
		if updateProgress {
			if err = batch.Commit(); err != nil {
				return err
			}
			if !useExternalTx {
				if err = s.Update(tx, stageProgress); err != nil {
					return err
				}
				if err = tx.Commit(); err != nil {
					return err
				}
				tx, err = cfg.db.BeginRw(context.Background())
				if err != nil {
					return err
				}
				// TODO: This creates stacked up deferrals
				defer tx.Rollback()
			}
			batch = kv.NewBatch(tx, quit)
			// TODO: This creates stacked up deferrals
			defer batch.Rollback()
		}

		gas = gas + block.GasUsed()

		select {
		default:
		case <-logEvery.C:
			logBlock, logTx, logTime = logProgress(logPrefix, logBlock, logTime, blockNum, logTx, lastLogTx, gas, batch)
			gas = 0
			tx.CollectMetrics()
			stageExecutionGauge.Update(int64(blockNum))
		}
	}

	if err = s.Update(batch, stageProgress); err != nil {
		return err
	}
	if err = batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	return stoppedErr
}

func pruneChangeSets(tx ethdb.RwTx, logPrefix string, table string, pruneTo uint64, logEvery *time.Ticker, ctx context.Context) error {
	c, err := tx.RwCursorDupSort(table)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

	for k, _, err := c.First(); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return fmt.Errorf("failed to move %s cleanup cursor: %w", table, err)
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= pruneTo {
			break
		}
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Mode", logPrefix), "table", table, "block", blockNum)
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		if err = c.DeleteCurrentDuplicates(); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
		}
	}
	return nil
}

func logProgress(logPrefix string, prevBlock uint64, prevTime time.Time, currentBlock uint64, prevTx, currentTx uint64, gas uint64, batch ethdb.DbWithPendingMutations) (uint64, uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentBlock-prevBlock) / (float64(interval) / float64(time.Second))
	speedTx := float64(currentTx-prevTx) / (float64(interval) / float64(time.Second))
	speedMgas := float64(gas) / 1_000_000 / (float64(interval) / float64(time.Second))
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentBlock,
		"blk/s", speed,
		"tx/s", speedTx,
		"Mgas/s", speedMgas,
	}
	if batch != nil {
		logpairs = append(logpairs, "batch", common.StorageSize(batch.BatchSize()))
	}
	logpairs = append(logpairs, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix), logpairs...)

	return currentBlock, currentTx, currentTime
}

func UnwindExecutionStage(u *UnwindState, s *StageState, tx ethdb.RwTx, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
	quit := ctx.Done()
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err = unwindExecutionStage(u, s, tx, quit, cfg, initialCycle); err != nil {
		return err
	}
	if err = u.Done(tx); err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg ExecuteBlockCfg, initialCycle bool) error {
	logPrefix := s.LogPrefix()
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength

	var accumulator *shards.Accumulator
	if !initialCycle && cfg.stateStream {
		hash, err := rawdb.ReadCanonicalHash(tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("%s: reading canonical hash of unwind point: %v", logPrefix, err)
		}
		accumulator.StartChange(u.UnwindPoint, hash, true /* unwind */)
	}

	changes := etl.NewCollector(cfg.tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer changes.Close(logPrefix)
	errRewind := changeset.RewindData(tx, s.BlockNumber, u.UnwindPoint, changes, quit)
	if errRewind != nil {
		return fmt.Errorf("%s: getting rewind data: %v", logPrefix, errRewind)
	}

	if err := changes.Load(logPrefix, tx, stateBucket, func(k []byte, value []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == 20 {
			if len(value) > 0 {
				var acc accounts.Account
				if err := acc.DecodeForStorage(value); err != nil {
					return err
				}

				// Fetch the code hash
				recoverCodeHashPlain(&acc, tx, k)
				var address common.Address
				copy(address[:], k)

				// cleanup contract code bucket
				original, err := state.NewPlainStateReader(tx).ReadAccountData(address)
				if err != nil {
					return fmt.Errorf("%s: read account for %x: %w", logPrefix, address, err)
				}
				if original != nil {
					// clean up all the code incarnations original incarnation and the new one
					for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
						err = tx.Delete(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], incarnation), nil)
						if err != nil {
							return fmt.Errorf("%s: writeAccountPlain for %x: %w", logPrefix, address, err)
						}
					}
				}

				newV := make([]byte, acc.EncodingLengthForStorage())
				acc.EncodeForStorage(newV)
				if accumulator != nil {
					accumulator.ChangeAccount(address, newV)
				}
				if err := next(k, k, newV); err != nil {
					return err
				}
			} else {
				if accumulator != nil {
					var address common.Address
					copy(address[:], k)
					accumulator.DeleteAccount(address)
				}
				if err := next(k, k, nil); err != nil {
					return err
				}
			}
			return nil
		}
		if accumulator != nil {
			var address common.Address
			var incarnation uint64
			var location common.Hash
			copy(address[:], k[:common.AddressLength])
			incarnation = binary.BigEndian.Uint64(k[common.AddressLength:])
			copy(location[:], k[common.AddressLength+common.IncarnationLength:])
			accumulator.ChangeStorage(address, incarnation, location, common.CopyBytes(value))
		}
		if len(value) > 0 {
			if err := next(k, k[:storageKeyLength], value); err != nil {
				return err
			}
		} else {
			if err := next(k, k[:storageKeyLength], nil); err != nil {
				return err
			}
		}
		return nil

	}, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	if err := changeset.Truncate(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := rawdb.DeleteNewerReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("%s: walking receipts: %v", logPrefix, err)
	}

	// Truncate CallTraceSet
	keyStart := dbutils.EncodeBlockNumber(u.UnwindPoint + 1)
	c, err := tx.RwCursorDupSort(dbutils.CallTraceSet)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		err = c.DeleteCurrentDuplicates()
		if err != nil {
			return err
		}
	}

	return nil
}

func recoverCodeHashPlain(acc *accounts.Account, db ethdb.Tx, key []byte) {
	var address common.Address
	copy(address[:], key)
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func PruneExecutionStage(s *PruneState, tx ethdb.RwTx, cfg ExecuteBlockCfg, ctx context.Context, initialCycle bool) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if cfg.prune.History.Enabled() {
		if err = pruneChangeSets(tx, logPrefix, dbutils.AccountChangeSetBucket, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
			return err
		}
		if err = pruneChangeSets(tx, logPrefix, dbutils.StorageChangeSetBucket, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
			return err
		}
	}

	if cfg.prune.Receipts.Enabled() {
		if err = pruneReceipts(tx, logPrefix, cfg.prune.Receipts.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
			return err
		}
	}
	if cfg.prune.CallTraces.Enabled() {
		if err = pruneCallTracesSet(tx, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
			return err
		}
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
		}
	}
	return nil
}

func pruneReceipts(tx ethdb.RwTx, logPrefix string, pruneTo uint64, logEvery *time.Ticker, ctx context.Context) error {
	c, err := tx.RwCursor(dbutils.Receipts)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= pruneTo {
			break
		}
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Mode", logPrefix), "table", dbutils.Receipts, "block", blockNum)
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		if err = c.DeleteCurrent(); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
		}
	}

	c, err = tx.RwCursor(dbutils.Log)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= pruneTo {
			break
		}
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Mode", logPrefix), "table", dbutils.Log, "block", blockNum)
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		if err = c.DeleteCurrent(); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
		}
	}
	return nil
}

func pruneCallTracesSet(tx ethdb.RwTx, logPrefix string, pruneTo uint64, logEvery *time.Ticker, ctx context.Context) error {
	c, err := tx.RwCursorDupSort(dbutils.CallTraceSet)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

	for k, _, err := c.First(); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= pruneTo {
			break
		}
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Mode", logPrefix), "table", dbutils.CallTraceSet, "block", blockNum)
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		if err = c.DeleteCurrentDuplicates(); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
		}
	}
	return nil
}
