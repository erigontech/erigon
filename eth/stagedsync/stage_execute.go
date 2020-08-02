package stagedsync

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
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
	logInterval = 30 // seconds
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

func SpawnExecuteBlocksStage(s *StageState, stateDB ethdb.Database, chainConfig *params.ChainConfig, chainContext core.ChainContext, vmConfig *vm.Config, toBlock uint64, quit <-chan struct{}, writeReceipts bool, changeSetHook ChangeSetHook) error {
	prevStageProgress, _, errStart := stages.GetStageProgress(stateDB, stages.Senders)
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
	log.Info("Blocks execution", "from", s.BlockNumber, "to", to)

	if prof {
		f, err := os.Create(fmt.Sprintf("cpu-%d.prof", s.BlockNumber))
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
	}

	batch := stateDB.NewBatch()

	engine := chainContext.Engine()

	stageProgress := s.BlockNumber
	logTime, logBlock := time.Now(), stageProgress

	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		stageProgress = blockNum

		blockHash := rawdb.ReadCanonicalHash(stateDB, blockNum)
		block := rawdb.ReadBlock(stateDB, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(stateDB, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

		var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets

		stateReader = state.NewPlainStateReader(batch)
		stateWriter = state.NewPlainStateWriter(batch, blockNum)

		// where the magic happens
		receipts, err := core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter)
		if err != nil {
			return err
		}

		if writeReceipts {
			rawdb.WriteReceipts(batch, block.Hash(), block.NumberU64(), receipts)
		}

		if batch.BatchSize() >= stateDB.IdealBatchSize() {
			if err = s.Update(batch, blockNum); err != nil {
				return err
			}
			start := time.Now()
			sz := batch.BatchSize()
			if _, err = batch.Commit(); err != nil {
				return err
			}
			log.Info("Batch committed", "in", time.Since(start), "size", common.StorageSize(sz))
		}

		if prof {
			if blockNum-s.BlockNumber == 100000 {
				// Flush the CPU profiler
				pprof.StopCPUProfile()

				// And the memory profiler
				f, _ := os.Create(fmt.Sprintf("mem-%d.prof", s.BlockNumber))
				runtime.GC()
				if err = pprof.WriteHeapProfile(f); err != nil {
					log.Error("could not save memory profile", "error", err)
				}
				f.Close()
			}
		}

		if changeSetHook != nil {
			if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
				changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
			}
		}

		logTime, logBlock = logProgress(logTime, logBlock, blockNum, batch)
	}

	if err := s.Update(batch, stageProgress); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("sync Execute: failed to write batch commit: %v", err)
	}
	log.Info("Completed on", "block", stageProgress)
	s.Done()
	return nil
}

func logProgress(lastLogTime time.Time, prev, now uint64, batch ethdb.DbWithPendingMutations) (time.Time, uint64) {
	if now%64 != 0 || time.Since(lastLogTime).Seconds() < logInterval {
		return lastLogTime, prev // return old values because no logging happened
	}

	speed := float64(now-prev) / float64(logInterval)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Executed blocks:",
		"currentBlock", now,
		"speed (blk/second)", speed,
		"state batch", common.StorageSize(batch.BatchSize()),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))

	return time.Now(), now
}

func UnwindExecutionStage(u *UnwindState, s *StageState, stateDB ethdb.Database, writeReceipts bool) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}

	log.Info("Unwind Execution stage", "from", s.BlockNumber, "to", u.UnwindPoint)
	batch := stateDB.NewBatch()

	rewindFunc := ethdb.RewindDataPlain
	stateBucket := dbutils.PlainStateBucket
	accountChangeSetBucket := dbutils.PlainAccountChangeSetBucket
	storageChangeSetBucket := dbutils.PlainStorageChangeSetBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength
	deleteAccountFunc := deleteAccountPlain
	writeAccountFunc := writeAccountPlain
	recoverCodeHashFunc := recoverCodeHashPlain

	accountMap, storageMap, err := rewindFunc(stateDB, s.BlockNumber, u.UnwindPoint)
	if err != nil {
		return fmt.Errorf("unwind Execution: getting rewind data: %v", err)
	}

	for key, value := range accountMap {
		if len(value) > 0 {
			var acc accounts.Account
			if err = acc.DecodeForStorage(value); err != nil {
				return err
			}

			// Fetch the code hash
			recoverCodeHashFunc(&acc, stateDB, key)
			if err = writeAccountFunc(batch, key, acc); err != nil {
				return err
			}
		} else {
			if err = deleteAccountFunc(batch, key); err != nil {
				return err
			}
		}
	}
	for key, value := range storageMap {
		if len(value) > 0 {
			if err = batch.Put(stateBucket, []byte(key)[:storageKeyLength], value); err != nil {
				return err
			}
		} else {
			if err = batch.Delete(stateBucket, []byte(key)[:storageKeyLength]); err != nil {
				return err
			}
		}
	}

	for i := s.BlockNumber; i > u.UnwindPoint; i-- {
		if err = deleteChangeSets(batch, i, accountChangeSetBucket, storageChangeSetBucket); err != nil {
			return err
		}
		if writeReceipts {
			blockHash := rawdb.ReadCanonicalHash(batch, i)
			rawdb.DeleteReceipts(batch, blockHash, i)
		}
	}

	if err = u.Done(batch); err != nil {
		return fmt.Errorf("unwind Execution: reset: %v", err)
	}

	_, err = batch.Commit()
	if err != nil {
		return fmt.Errorf("unwind Execute: failed to write db commit: %v", err)
	}
	return nil
}

func writeAccountHashed(db ethdb.Database, key string, acc accounts.Account) error {
	var addrHash common.Hash
	copy(addrHash[:], []byte(key))
	if err := cleanupContractCodeBucket(
		db,
		dbutils.ContractCodeBucket,
		acc,
		func(db ethdb.Getter, out *accounts.Account) (bool, error) {
			return rawdb.ReadAccount(db, addrHash, out)
		},
		func(inc uint64) []byte { return dbutils.GenerateStoragePrefix(addrHash[:], inc) },
	); err != nil {
		return err
	}
	return rawdb.WriteAccount(db, addrHash, acc)
}

func writeAccountPlain(db ethdb.Database, key string, acc accounts.Account) error {
	var address common.Address
	copy(address[:], []byte(key))
	if err := cleanupContractCodeBucket(
		db,
		dbutils.PlainContractCodeBucket,
		acc,
		func(db ethdb.Getter, out *accounts.Account) (bool, error) {
			return rawdb.PlainReadAccount(db, address, out)
		},
		func(inc uint64) []byte { return dbutils.PlainGenerateStoragePrefix(address[:], inc) },
	); err != nil {
		return fmt.Errorf("writeAccountPlain for %x: %w", address, err)
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
	db ethdb.Database,
	bucket []byte,
	acc accounts.Account,
	readAccountFunc func(ethdb.Getter, *accounts.Account) (bool, error),
	getKeyForIncarnationFunc func(uint64) []byte,
) error {
	var original accounts.Account
	got, err := readAccountFunc(db, &original)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return fmt.Errorf("cleanupContractCodeBucket: %w", err)
	}
	if got {
		// clean up all the code incarnations original incarnation and the new one
		for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
			err = db.Delete(bucket, getKeyForIncarnationFunc(incarnation))
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

func deleteChangeSets(batch ethdb.Deleter, timestamp uint64, accountBucket, storageBucket []byte) error {
	changeSetKey := dbutils.EncodeTimestamp(timestamp)
	if err := batch.Delete(accountBucket, changeSetKey); err != nil {
		return err
	}
	if err := batch.Delete(storageBucket, changeSetKey); err != nil {
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
