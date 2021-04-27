package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/silkworm"
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

type StateWriterBuilder func(db ethdb.Database, changeSetsDB ethdb.Database, blockNumber uint64) state.WriterWithChangeSets

type ExecuteBlockCfg struct {
	writeReceipts         bool
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
	WriteReceipts bool,
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
		writeReceipts:         WriteReceipts,
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

func readBlock(blockNum uint64, tx ethdb.Getter) (*types.Block, error) {
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, _, err := rawdb.ReadBlockWithSenders(tx, blockHash, blockNum)
	return b, err
}

func executeBlockWithGo(block *types.Block, tx ethdb.Database, batch ethdb.Database, params ExecuteBlockCfg) error {
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
		stateWriter = state.NewPlainStateWriter(batch, tx, blockNum)
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
	receipts, err := core.ExecuteBlockEphemerally(params.chainConfig, params.vmConfig, getHeader, params.engine, block, stateReader, stateWriter)
	if err != nil {
		return err
	}

	if params.writeReceipts {
		if err = rawdb.AppendReceipts(tx.(ethdb.HasTx).Tx().(ethdb.RwTx), blockNum, receipts); err != nil {
			return err
		}
	}

	if params.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			params.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}

	return nil
}

func SpawnExecuteBlocksStage(s *StageState, stateDB ethdb.Database, toBlock uint64, quit <-chan struct{}, params ExecuteBlockCfg) error {
	prevStageProgress, errStart := stages.GetStageProgress(stateDB, stages.Senders)
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

	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := stateDB.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = stateDB.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = stateDB.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	useSilkworm := params.silkwormExecutionFunc != nil
	if useSilkworm && params.changeSetHook != nil {
		panic("ChangeSetHook is not supported with Silkworm")
	}

	var batch ethdb.DbWithPendingMutations
	useBatch := !useSilkworm
	if useBatch {
		batch = tx.NewBatch()
		defer batch.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress
	logTime := time.Now()

	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		err := common.Stopped(quit)
		if err != nil {
			return err
		}
		if useSilkworm {
			txn := tx.(ethdb.HasTx).Tx()
			// Silkworm executes many blocks simultaneously
			if blockNum, err = silkworm.ExecuteBlocks(params.silkwormExecutionFunc, txn, params.chainConfig.ChainID, blockNum, to, int(params.batchSize), params.writeReceipts); err != nil {
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
			if err = executeBlockWithGo(block, tx, batch, params); err != nil {
				return err
			}
		}

		stageProgress = blockNum

		updateProgress := !useBatch || batch.BatchSize() >= int(params.batchSize)
		if updateProgress {
			if err = s.Update(tx, stageProgress); err != nil {
				return err
			}
			if useBatch {
				if err = batch.CommitAndBegin(context.Background()); err != nil {
					return err
				}
			}
			if !useExternalTx {
				if err = tx.CommitAndBegin(context.Background()); err != nil {
					return err
				}
			}
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
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	s.Done()
	return nil
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

func UnwindExecutionStage(u *UnwindState, s *StageState, stateDB ethdb.Database, quit <-chan struct{}, params ExecuteBlockCfg) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := stateDB.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = stateDB.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = stateDB.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err := unwindExecutionStage(u, s, tx.(ethdb.HasTx).Tx().(ethdb.RwTx), quit, params); err != nil {
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

func unwindExecutionStage(u *UnwindState, s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg ExecuteBlockCfg) error {
	logPrefix := s.state.LogPrefix()
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength

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
				if err := next(k, k, newV); err != nil {
					return err
				}
			} else {
				if err := next(k, k, nil); err != nil {
					return err
				}
			}
			return nil
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
