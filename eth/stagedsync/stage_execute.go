package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

const (
	logInterval = 30 * time.Second
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type StateReaderBuilder func(ethdb.Getter) state.StateReader

type StateWriterBuilder func(db ethdb.Database, changeSetsDB ethdb.Database, blockNumber uint64) state.WriterWithChangeSets

type ExecuteBlockStageParams struct {
	ToBlock       uint64 // not setting this params means no limit
	WriteReceipts bool
	BatchSize     int
	ChangeSetHook ChangeSetHook
	ReaderBuilder StateReaderBuilder
	WriterBuilder StateWriterBuilder
}

func SpawnExecuteBlocksStage(s *StageState, stateDB ethdb.Database, chainConfig *params.ChainConfig, chainContext *core.TinyChainContext, vmConfig *vm.Config, quit <-chan struct{}, params ExecuteBlockStageParams) error {
	prevStageProgress, _, errStart := stages.GetStageProgress(stateDB, stages.Senders)
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

	batch := tx.NewBatch()
	defer batch.Rollback()

	engine := chainContext.Engine()
	chainContext.SetDB(tx)

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress

	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		stageProgress = blockNum

		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		block := rawdb.ReadBlock(tx, blockHash, blockNum)
		if block == nil {
			log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "hash", blockHash.String(), "blocknum", blockNum)
			break
		}
		senders := rawdb.ReadSenders(tx, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

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
		receipts, err := core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter)
		if err != nil {
			return err
		}

		if params.WriteReceipts {
			if err = rawdb.AppendReceipts(tx, block.NumberU64(), receipts); err != nil {
				return err
			}
		}

		if batch.BatchSize() >= params.BatchSize {
			if err = s.Update(batch, blockNum); err != nil {
				return err
			}
			if err = batch.CommitAndBegin(context.Background()); err != nil {
				return err
			}
			if !useExternalTx {
				if err = tx.CommitAndBegin(context.Background()); err != nil {
					return err
				}
				chainContext.SetDB(tx)
			}
		}

		if params.ChangeSetHook != nil {
			if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
				params.ChangeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
			}
		}

		select {
		default:
		case <-logEvery.C:
			logBlock = logProgress(logPrefix, logBlock, blockNum, batch)
		}
	}

	if err := s.Update(batch, stageProgress); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	s.Done()
	return nil
}

func logProgress(logPrefix string, prev, now uint64, batch ethdb.DbWithPendingMutations) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix),
		"number", now,
		"blk/second", speed,
		"batch", common.StorageSize(batch.BatchSize()),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))

	return now
}

func UnwindExecutionStage(u *UnwindState, s *StageState, stateDB ethdb.Database, writeReceipts bool) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}

	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)
	batch := stateDB.NewBatch()
	defer batch.Rollback()

	rewindFunc := ethdb.RewindDataPlain
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength
	deleteAccountFunc := deleteAccountPlain
	writeAccountFunc := writeAccountPlain
	recoverCodeHashFunc := recoverCodeHashPlain

	accountMap, storageMap, errRewind := rewindFunc(stateDB, s.BlockNumber, u.UnwindPoint)
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
			recoverCodeHashFunc(&acc, stateDB, key)
			if err := writeAccountFunc(logPrefix, batch, key, acc); err != nil {
				return err
			}
		} else {
			if err := deleteAccountFunc(batch, key); err != nil {
				return err
			}
		}
	}
	for key, value := range storageMap {
		if len(value) > 0 {
			if err := batch.Put(stateBucket, []byte(key)[:storageKeyLength], value); err != nil {
				return err
			}
		} else {
			if err := batch.Delete(stateBucket, []byte(key)[:storageKeyLength], nil); err != nil {
				return err
			}
		}
	}

	if err := stateDB.Walk(dbutils.PlainAccountChangeSetBucket, dbutils.EncodeTimestamp(u.UnwindPoint+1), 0, func(k, _ []byte) (bool, error) {
		if err1 := batch.Delete(dbutils.PlainAccountChangeSetBucket, common.CopyBytes(k), nil); err1 != nil {
			return false, fmt.Errorf("%s: delete account changesets: %v", logPrefix, err1)
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("%s: walking account changesets: %v", logPrefix, err)
	}
	if err := stateDB.Walk(dbutils.PlainStorageChangeSetBucket, dbutils.EncodeTimestamp(u.UnwindPoint+1), 0, func(k, _ []byte) (bool, error) {
		if err1 := batch.Delete(dbutils.PlainStorageChangeSetBucket, common.CopyBytes(k), nil); err1 != nil {
			return false, fmt.Errorf("%s: delete storage changesets: %v", logPrefix, err1)
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("%s: walking storage changesets: %v", logPrefix, err)
	}
	if writeReceipts {
		if err := rawdb.DeleteNewerReceipts(stateDB, u.UnwindPoint+1); err != nil {
			return fmt.Errorf("%s: walking receipts: %v", logPrefix, err)
		}
	}

	if err := u.Done(batch); err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}

	_, err := batch.Commit()
	if err != nil {
		return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
	}
	return nil
}

func writeAccountPlain(logPrefix string, db ethdb.Database, key string, acc accounts.Account) error {
	var address common.Address
	copy(address[:], []byte(key))
	if err := cleanupContractCodeBucket(
		logPrefix,
		db,
		dbutils.PlainContractCodeBucket,
		acc,
		func(db ethdb.Getter, out *accounts.Account) (bool, error) {
			return rawdb.PlainReadAccount(db, address, out)
		},
		func(inc uint64) []byte { return dbutils.PlainGenerateStoragePrefix(address[:], inc) },
	); err != nil {
		return fmt.Errorf("%s: writeAccountPlain for %x: %w", logPrefix, address, err)
	}

	return rawdb.PlainWriteAccount(db, address, acc)
}

func recoverCodeHashHashed(acc *accounts.Account, db ethdb.Getter, key string) {
	var addrHash common.Hash
	copy(addrHash[:], []byte(key))
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func cleanupContractCodeBucket(
	logPrefix string,
	db ethdb.Database,
	bucket string,
	acc accounts.Account,
	readAccountFunc func(ethdb.Getter, *accounts.Account) (bool, error),
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

func recoverCodeHashPlain(acc *accounts.Account, db ethdb.Getter, key string) {
	var address common.Address
	copy(address[:], []byte(key))
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.Get(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func deleteAccountHashed(db rawdb.DatabaseDeleter, key string) error {
	var addrHash common.Hash
	copy(addrHash[:], []byte(key))
	return rawdb.DeleteAccount(db, addrHash)
}

func deleteAccountPlain(db rawdb.DatabaseDeleter, key string) error {
	var address common.Address
	copy(address[:], []byte(key))
	return rawdb.PlainDeleteAccount(db, address)
}

func deleteChangeSets(batch ethdb.Deleter, timestamp uint64, accountBucket, storageBucket string) error {
	changeSetKey := dbutils.EncodeTimestamp(timestamp)
	if err := batch.Delete(accountBucket, changeSetKey, nil); err != nil {
		return err
	}
	if err := batch.Delete(storageBucket, changeSetKey, nil); err != nil {
		return err
	}
	return nil
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
