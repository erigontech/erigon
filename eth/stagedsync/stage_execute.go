package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
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
	"github.com/ledgerwatch/erigon/turbo/silkworm"
)

var stageExecutionGauge = metrics.NewRegisteredGauge("stage/execution", nil)

const (
	logInterval = 30 * time.Second
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type StateReaderBuilder func(ethdb.Database) state.StateReader

type StateWriterBuilder func(db ethdb.Database, changeSetsDB ethdb.RwTx, blockNumber uint64) state.WriterWithChangeSets

type ExecuteBlockCfg struct {
	db                    ethdb.RwKV
	writeReceipts         bool
	writeCallTraces       bool
	pruningDistance       uint64
	batchSize             datasize.ByteSize
	changeSetHook         ChangeSetHook
	readerBuilder         StateReaderBuilder
	writerBuilder         StateWriterBuilder
	silkwormExecutionFunc unsafe.Pointer
	chainConfig           *params.ChainConfig
	engine                consensus.Engine
	vmConfig              *vm.Config
	tmpdir                string
}

func StageExecuteBlocksCfg(
	kv ethdb.RwKV,
	WriteReceipts bool,
	WriteCallTraces bool,
	pruningDistance uint64,
	BatchSize datasize.ByteSize,
	ReaderBuilder StateReaderBuilder,
	WriterBuilder StateWriterBuilder,
	SilkwormExecutionFunc unsafe.Pointer,
	ChangeSetHook ChangeSetHook,
	chainConfig *params.ChainConfig,
	engine consensus.Engine,
	vmConfig *vm.Config,
	tmpdir string,
) ExecuteBlockCfg {
	return ExecuteBlockCfg{
		db:                    kv,
		writeReceipts:         WriteReceipts,
		writeCallTraces:       WriteCallTraces,
		pruningDistance:       pruningDistance,
		batchSize:             BatchSize,
		changeSetHook:         ChangeSetHook,
		readerBuilder:         ReaderBuilder,
		writerBuilder:         WriterBuilder,
		silkwormExecutionFunc: SilkwormExecutionFunc,
		chainConfig:           chainConfig,
		engine:                engine,
		vmConfig:              vmConfig,
		tmpdir:                tmpdir,
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

func executeBlockWithGo(
	block *types.Block,
	tx ethdb.RwTx,
	batch ethdb.Database,
	params ExecuteBlockCfg,
	writeChangesets bool,
	traceCursor ethdb.RwCursorDupSort,
	accumulator *shards.Accumulator,
) error {
	blockNum := block.NumberU64()
	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	if params.readerBuilder != nil {
		stateReader = params.readerBuilder(batch)
	} else {
		stateReader = state.NewPlainStateReader(batch)
	}

	if params.writerBuilder != nil {
		stateWriter = params.writerBuilder(batch, tx, blockNum)
	} else {
		if accumulator != nil {
			accumulator.StartChange(blockNum, block.Hash(), false /* unwind */)
		}
		if writeChangesets {
			stateWriter = state.NewPlainStateWriter(batch, tx, blockNum).SetAccumulator(accumulator)
		} else {
			stateWriter = state.NewPlainStateWriterNoHistory(batch, blockNum).SetAccumulator(accumulator)
		}
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
	var callTracer *CallTracer
	if params.writeCallTraces {
		callTracer = NewCallTracer()
		params.vmConfig.Debug = true
		params.vmConfig.Tracer = callTracer
	}
	receipts, err := core.ExecuteBlockEphemerally(params.chainConfig, params.vmConfig, getHeader, params.engine, block, stateReader, stateWriter)
	if err != nil {
		return err
	}

	if params.writeReceipts {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}
	}

	if params.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			params.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}

	if params.writeCallTraces {
		callTracer.tos[block.Coinbase()] = struct{}{}
		for _, uncle := range block.Uncles() {
			callTracer.tos[uncle.Coinbase] = struct{}{}
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
			if j == 0 {
				if err = traceCursor.Append(blockNumEnc[:], v[:]); err != nil {
					return err
				}
			} else {
				if err = traceCursor.AppendDup(blockNumEnc[:], v[:]); err != nil {
					return err
				}
			}
			copy(prev[:], addr[:])
		}
	}

	return nil
}

func SpawnExecuteBlocksStage(s *StageState, tx ethdb.RwTx, toBlock uint64, quit <-chan struct{}, cfg ExecuteBlockCfg, accumulator *shards.Accumulator) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
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
	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to <= s.BlockNumber {
		s.Done()
		return nil
	}
	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)

	var traceCursor ethdb.RwCursorDupSort
	if cfg.writeCallTraces {
		var err error
		if traceCursor, err = tx.RwCursorDupSort(dbutils.CallTraceSet); err != nil {
			return fmt.Errorf("%s: failed to create cursor for call traces: %v", logPrefix, err)
		}
		defer traceCursor.Close()
	}

	useSilkworm := cfg.silkwormExecutionFunc != nil
	if useSilkworm && cfg.changeSetHook != nil {
		panic("ChangeSetHook is not supported with Silkworm")
	}

	var batch ethdb.DbWithPendingMutations
	useBatch := !useSilkworm
	if useBatch {
		batch = ethdb.NewBatch(tx)
		defer batch.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress
	logTime := time.Now()

	var stoppedErr error
	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			break
		}
		var err error
		if useSilkworm {
			// Silkworm executes many blocks simultaneously
			if blockNum, err = silkworm.ExecuteBlocks(cfg.silkwormExecutionFunc, tx, cfg.chainConfig.ChainID, blockNum, to, int(cfg.batchSize), cfg.writeReceipts); err != nil {
				return err
			}
		} else {
			var block *types.Block
			if block, err = readBlock(blockNum, tx); err != nil {
				return err
			}
			if block == nil {
				log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
				break
			}
			writeChangesets := true
			if cfg.pruningDistance > 0 && to-blockNum > cfg.pruningDistance {
				writeChangesets = false
			}
			if err = executeBlockWithGo(block, tx, batch, cfg, writeChangesets, traceCursor, accumulator); err != nil {
				return err
			}
		}

		stageProgress = blockNum

		updateProgress := !useBatch || batch.BatchSize() >= int(cfg.batchSize)
		if updateProgress {
			if useBatch {
				if err = batch.Commit(); err != nil {
					return err
				}
			}
			if !useExternalTx {
				if err = s.Update(tx, stageProgress); err != nil {
					return err
				}
				if traceCursor != nil {
					traceCursor.Close()
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
				if cfg.writeCallTraces {
					if traceCursor, err = tx.RwCursorDupSort(dbutils.CallTraceSet); err != nil {
						return fmt.Errorf("%s: failed to create cursor for call traces: %v", logPrefix, err)
					}
				}
			}
			batch = ethdb.NewBatch(tx)
			// TODO: This creates stacked up deferrals
			defer batch.Rollback()
		}

		select {
		default:
		case <-logEvery.C:
			logBlock, logTime = logProgress(logPrefix, logBlock, logTime, blockNum, batch)
			if hasTx, ok := tx.(ethdb.HasTx); ok {
				hasTx.Tx().CollectMetrics()
			}
		}
		stageExecutionGauge.Update(int64(blockNum))
	}

	if useBatch {
		if err := s.Update(batch, stageProgress); err != nil {
			return err
		}
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
		}
	}
	if !useExternalTx {
		if traceCursor != nil {
			traceCursor.Close()
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	s.Done()
	return stoppedErr
}

func logProgress(logPrefix string, prevBlock uint64, prevTime time.Time, currentBlock uint64, batch ethdb.DbWithPendingMutations) (uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentBlock-prevBlock) / float64(interval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentBlock,
		"blk/second", speed,
	}
	if batch != nil {
		logpairs = append(logpairs, "batch", common.StorageSize(batch.BatchSize()))
	}
	logpairs = append(logpairs, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys), "numGC", int(m.NumGC))
	log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix), logpairs...)

	return currentBlock, currentTime
}

func UnwindExecutionStage(u *UnwindState, s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg ExecuteBlockCfg, accumulator *shards.Accumulator) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err := unwindExecutionStage(u, s, tx, quit, cfg, accumulator); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg ExecuteBlockCfg, accumulator *shards.Accumulator) error {
	logPrefix := s.state.LogPrefix()
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength

	if accumulator != nil {
		hash, err := rawdb.ReadCanonicalHash(tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("%s: reading canonical hash of unwind point: %v", logPrefix, err)
		}
		accumulator.StartChange(u.UnwindPoint, hash, true /* unwind */)
	}
	changes, errRewind := changeset.RewindData(tx, s.BlockNumber, u.UnwindPoint, cfg.tmpdir, quit)
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
				if err := cleanupContractCodeBucket(
					logPrefix,
					tx,
					dbutils.PlainContractCodeBucket,
					acc,
					func(db ethdb.Tx, out *accounts.Account) (bool, error) {
						return rawdb.PlainReadAccount(db, address, out)
					},
					func(inc uint64) []byte { return dbutils.PlainGenerateStoragePrefix(address[:], inc) },
				); err != nil {
					return fmt.Errorf("%s: writeAccountPlain for %x: %w", logPrefix, address, err)
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

	if cfg.writeReceipts {
		if err := rawdb.DeleteNewerReceipts(tx, u.UnwindPoint+1); err != nil {
			return fmt.Errorf("%s: walking receipts: %v", logPrefix, err)
		}
	}

	if cfg.writeCallTraces {
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
	}

	return nil
}

func cleanupContractCodeBucket(
	logPrefix string,
	db ethdb.RwTx,
	bucket string,
	acc accounts.Account,
	readAccountFunc func(ethdb.Tx, *accounts.Account) (bool, error),
	getKeyForIncarnationFunc func(uint64) []byte,
) error {
	var original accounts.Account
	got, err := readAccountFunc(db, &original)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return fmt.Errorf("%s: cleanupContractCodeBucket: %w", logPrefix, err)
	}
	if got {
		// clean up all the code incarnations original incarnation and the new one
		for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
			err = db.Delete(bucket, getKeyForIncarnationFunc(incarnation), nil)
			if err != nil {
				return err
			}
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
