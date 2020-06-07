package stagedsync

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"

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
)

const (
	logInterval = 30 // seconds
)

type progressLogger struct {
	timer    *time.Ticker
	quit     chan struct{}
	interval int
	batch    ethdb.DbWithPendingMutations
}

func NewProgressLogger(intervalInSeconds int, batch ethdb.DbWithPendingMutations) *progressLogger {
	return &progressLogger{
		timer:    time.NewTicker(time.Duration(intervalInSeconds) * time.Second),
		quit:     make(chan struct{}),
		interval: intervalInSeconds,
		batch:    batch,
	}
}

func (l *progressLogger) Start(numberRef *uint64) {
	go func() {
		prev := atomic.LoadUint64(numberRef)
		printFunc := func() {
			now := atomic.LoadUint64(numberRef)
			speed := float64(now-prev) / float64(l.interval)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Executed blocks:",
				"currentBlock", now,
				"speed (blk/second)", speed,
				"state batch", common.StorageSize(l.batch.BatchSize()),
				"alloc", int(m.Alloc/1024),
				"sys", int(m.Sys/1024),
				"numGC", int(m.NumGC))

			prev = now
		}
		for {
			select {
			case <-l.timer.C:
				printFunc()
			case <-l.quit:
				printFunc()
				return
			}
		}
	}()
}

func (l *progressLogger) Stop() {
	l.timer.Stop()
	close(l.quit)
}

const StateBatchSize = 50 * 1024 * 1024 // 50 Mb
const ChangeBatchSize = 1024 * 2014     // 1 Mb
const prof = false

func SpawnExecuteBlocksStage(s *StageState, stateDB ethdb.Database, blockchain BlockChain, limit uint64, quit chan struct{}, dests vm.Cache) error {
	lastProcessedBlockNumber := s.BlockNumber

	nextBlockNumber := uint64(0)

	atomic.StoreUint64(&nextBlockNumber, lastProcessedBlockNumber+1)
	profileNumber := atomic.LoadUint64(&nextBlockNumber)
	if prof {
		f, err := os.Create(fmt.Sprintf("cpu-%d.prof", profileNumber))
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
	}

	stateBatch := stateDB.NewBatch()
	changeBatch := stateDB.NewBatch()

	progressLogger := NewProgressLogger(logInterval, stateBatch)
	progressLogger.Start(&nextBlockNumber)
	defer progressLogger.Stop()

	accountCache := fastcache.New(128 * 1024 * 1024) // 128 Mb
	storageCache := fastcache.New(128 * 1024 * 1024) // 128 Mb
	codeCache := fastcache.New(32 * 1024 * 1024)     // 32 Mb (the minimum)
	codeSizeCache := fastcache.New(32 * 1024 * 1024) // 32 Mb (the minimum)

	chainConfig := blockchain.Config()
	engine := blockchain.Engine()
	vmConfig := blockchain.GetVMConfig()
	for {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		blockNum := atomic.LoadUint64(&nextBlockNumber)
		if limit > 0 && blockNum >= limit {
			break
		}

		block := blockchain.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}

		type cacheSetter interface {
			SetAccountCache(cache *fastcache.Cache)
			SetStorageCache(cache *fastcache.Cache)
			SetCodeCache(cache *fastcache.Cache)
			SetCodeSizeCache(cache *fastcache.Cache)
		}

		var stateReader interface {
			state.StateReader
			cacheSetter
		}
		var stateWriter interface {
			state.WriterWithChangeSets
			cacheSetter
		}
		if core.UsePlainStateExecution {
			stateReader = state.NewPlainStateReader(stateBatch)
			stateWriter = state.NewPlainStateWriter(stateBatch, changeBatch, blockNum)
		} else {
			stateReader = state.NewDbStateReader(stateBatch)
			stateWriter = state.NewDbStateWriter(stateBatch, changeBatch, blockNum)
		}
		stateReader.SetAccountCache(accountCache)
		stateReader.SetStorageCache(storageCache)
		stateReader.SetCodeCache(codeCache)
		stateReader.SetCodeSizeCache(codeSizeCache)

		stateWriter.SetAccountCache(accountCache)
		stateWriter.SetStorageCache(storageCache)
		stateWriter.SetCodeCache(codeCache)
		stateWriter.SetCodeSizeCache(codeSizeCache)

		// where the magic happens
		err := core.ExecuteBlockEuphemerally(chainConfig, vmConfig, blockchain, engine, block, stateReader, stateWriter, dests)
		if err != nil {
			return err
		}

		if err = s.Update(stateBatch, blockNum); err != nil {
			return err
		}

		atomic.AddUint64(&nextBlockNumber, 1)

		if stateBatch.BatchSize() >= StateBatchSize {
			start := time.Now()
			if _, err = stateBatch.Commit(); err != nil {
				return err
			}
			log.Info("State batch committed", "in", time.Since(start))
		}
		if changeBatch.BatchSize() >= ChangeBatchSize {
			if _, err = changeBatch.Commit(); err != nil {
				return err
			}
		}

		if prof {
			if blockNum-profileNumber == 100000 {
				// Flush the profiler
				pprof.StopCPUProfile()
			}
		}
	}

	_, err := stateBatch.Commit()
	if err != nil {
		return fmt.Errorf("sync Execute: failed to write state batch commit: %v", err)
	}
	_, err = changeBatch.Commit()
	if err != nil {
		return fmt.Errorf("sync Execute: failed to write change batch commit: %v", err)
	}
	s.Done()
	return nil
}

func unwindExecutionStage(unwindPoint uint64, stateDB ethdb.Database) error {
	lastProcessedBlockNumber, err := stages.GetStageProgress(stateDB, stages.Execution)
	if err != nil {
		return fmt.Errorf("unwind Execution: get stage progress: %v", err)
	}

	if unwindPoint >= lastProcessedBlockNumber {
		err = stages.SaveStageUnwind(stateDB, stages.Execution, 0)
		if err != nil {
			return fmt.Errorf("unwind Execution: reset: %v", err)
		}
		return nil
	}
	log.Info("Unwind Execution stage", "from", lastProcessedBlockNumber, "to", unwindPoint)
	mutation := stateDB.NewBatch()

	rewindFunc := ethdb.RewindData
	stateBucket := dbutils.CurrentStateBucket
	accountChangeSetBucket := dbutils.AccountChangeSetBucket
	storageChangeSetBucket := dbutils.StorageChangeSetBucket
	storageKeyLength := common.HashLength + common.IncarnationLength + common.HashLength
	deleteAccountFunc := deleteAccountHashed
	writeAccountFunc := writeAccountHashed
	recoverCodeHashFunc := recoverCodeHashHashed

	if core.UsePlainStateExecution {
		rewindFunc = ethdb.RewindDataPlain
		stateBucket = dbutils.PlainStateBucket
		accountChangeSetBucket = dbutils.PlainAccountChangeSetBucket
		storageChangeSetBucket = dbutils.PlainStorageChangeSetBucket
		storageKeyLength = common.AddressLength + common.IncarnationLength + common.HashLength
		deleteAccountFunc = deleteAccountPlain
		writeAccountFunc = writeAccountPlain
		recoverCodeHashFunc = recoverCodeHashPlain
	}

	accountMap, storageMap, err := rewindFunc(stateDB, lastProcessedBlockNumber, unwindPoint)
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
			if err = writeAccountFunc(mutation, key, acc); err != nil {
				return err
			}
		} else {
			if err = deleteAccountFunc(mutation, key); err != nil {
				return err
			}
		}
	}
	for key, value := range storageMap {
		if len(value) > 0 {
			if err = mutation.Put(stateBucket, []byte(key)[:storageKeyLength], value); err != nil {
				return err
			}
		} else {
			if err = mutation.Delete(stateBucket, []byte(key)[:storageKeyLength]); err != nil {
				return err
			}
		}
	}

	for i := lastProcessedBlockNumber; i > unwindPoint; i-- {
		if err = deleteChangeSets(mutation, i, accountChangeSetBucket, storageChangeSetBucket); err != nil {
			return err
		}
	}

	err = stages.SaveStageUnwind(mutation, stages.Execution, 0)
	if err != nil {
		return fmt.Errorf("unwind Execution: reset: %v", err)
	}

	_, err = mutation.Commit()
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
		func(inc uint64) []byte { return dbutils.PlainGenerateStoragePrefix(address, inc) },
	); err != nil {
		return err
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
	if err != nil {
		return err
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
		if codeHash, err2 := db.Get(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address, acc.Incarnation)); err2 == nil {
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
