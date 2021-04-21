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

type ExecuteBlockStageParams struct {
	ToBlock               uint64 // not setting this params means no limit
	WriteReceipts         bool
	BatchSize             datasize.ByteSize
	ChangeSetHook         ChangeSetHook
	ReaderBuilder         StateReaderBuilder
	WriterBuilder         StateWriterBuilder
	SilkwormExecutionFunc unsafe.Pointer
}

func readBlock(blockNum uint64, tx ethdb.Getter) (*types.Block, error) {
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, _, err := rawdb.ReadBlockWithSenders(tx, blockHash, blockNum)
	return b, err
}

func executeBlockWithGo(block *types.Block, tx ethdb.Database, batch ethdb.Database, chainConfig *params.ChainConfig, engine consensus.Engine, vmConfig *vm.Config, params ExecuteBlockStageParams) error {

	blockNum := block.NumberU64()
	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	if params.ReaderBuilder != nil {
		stateReader = params.ReaderBuilder(batch)
	} else {
		stateReader = state.NewPlainStateReader(batch)
	}

	if params.WriterBuilder != nil {
		stateWriter = params.WriterBuilder(batch, tx, blockNum)
	} else {
		stateWriter = state.NewPlainStateWriter(batch, tx, blockNum)
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
	receipts, err := core.ExecuteBlockEphemerally(chainConfig, vmConfig, getHeader, engine, block, stateReader, stateWriter)
	if err != nil {
		return err
	}

	if params.WriteReceipts {
		if err = rawdb.AppendReceipts(tx.(ethdb.HasTx).Tx().(ethdb.RwTx), blockNum, receipts); err != nil {
			return err
		}
	}

	if params.ChangeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			params.ChangeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}

	return nil
}

func SpawnExecuteBlocksStage(s *StageState, stateDB ethdb.Database, chainConfig *params.ChainConfig, engine consensus.Engine, vmConfig *vm.Config, quit <-chan struct{}, params ExecuteBlockStageParams) error {
	prevStageProgress, errStart := stages.GetStageProgress(stateDB, stages.Senders)
	if errStart != nil {
		return errStart
	}
	var to = prevStageProgress
	if params.ToBlock > 0 {
		to = min(prevStageProgress, params.ToBlock)
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

	useSilkworm := params.SilkwormExecutionFunc != nil
	if useSilkworm && params.ChangeSetHook != nil {
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
			if blockNum, err = silkworm.ExecuteBlocks(params.SilkwormExecutionFunc, txn, chainConfig.ChainID, blockNum, to, int(params.BatchSize), params.WriteReceipts); err != nil {
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
			if err = executeBlockWithGo(block, tx, batch, chainConfig, engine, vmConfig, params); err != nil {
				return err
			}
		}

		stageProgress = blockNum

		updateProgress := !useBatch || batch.BatchSize() >= int(params.BatchSize)
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

func UnwindExecutionStage(u *UnwindState, s *StageState, stateDB ethdb.Database, quit <-chan struct{}, params ExecuteBlockStageParams) error {
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

func unwindExecutionStage(u *UnwindState, s *StageState, tx ethdb.RwTx, quit <-chan struct{}, params ExecuteBlockStageParams) error {
	logPrefix := s.state.LogPrefix()
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength

	accountMap, storageMap, errRewind := changeset.RewindData(tx, s.BlockNumber, u.UnwindPoint, quit)
	if errRewind != nil {
		return fmt.Errorf("%s: getting rewind data: %v", logPrefix, errRewind)
	}
	for key, value := range accountMap {
		if len(value) > 0 {
			var acc accounts.Account
			if err := acc.DecodeForStorage(value); err != nil {
				return err
			}

			// Fetch the code hash
			recoverCodeHashPlain(&acc, tx, key)
			if err := writeAccountPlain(logPrefix, tx, key, acc); err != nil {
				return err
			}
		} else {
			if err := deleteAccountPlain(tx, key); err != nil {
				return err
			}
		}
	}

	for key, value := range storageMap {
		k := []byte(key)
		if len(value) > 0 {
			if err := tx.Put(stateBucket, k[:storageKeyLength], value); err != nil {
				return err
			}
		} else {
			if err := tx.Delete(stateBucket, k[:storageKeyLength], nil); err != nil {
				return err
			}
		}
	}

	if err := changeset.Truncate(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if params.WriteReceipts {
		if err := rawdb.DeleteNewerReceipts(tx, u.UnwindPoint+1); err != nil {
			return fmt.Errorf("%s: walking receipts: %v", logPrefix, err)
		}
	}

	return nil
}

func writeAccountPlain(logPrefix string, db ethdb.RwTx, key string, acc accounts.Account) error {
	var address common.Address
	copy(address[:], key)
	if err := cleanupContractCodeBucket(
		logPrefix,
		db,
		dbutils.PlainContractCodeBucket,
		acc,
		func(db ethdb.Tx, out *accounts.Account) (bool, error) {
			return rawdb.PlainReadAccount(ethdb.NewRoTxDb(db), address, out)
		},
		func(inc uint64) []byte { return dbutils.PlainGenerateStoragePrefix(address[:], inc) },
	); err != nil {
		return fmt.Errorf("%s: writeAccountPlain for %x: %w", logPrefix, address, err)
	}

	return rawdb.PlainWriteAccount(db, address, acc)
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

func recoverCodeHashPlain(acc *accounts.Account, db ethdb.Tx, key string) {
	var address common.Address
	copy(address[:], key)
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func deleteAccountPlain(db ethdb.Deleter, key string) error {
	var address common.Address
	copy(address[:], key)
	return rawdb.PlainDeleteAccount(db, address)
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
