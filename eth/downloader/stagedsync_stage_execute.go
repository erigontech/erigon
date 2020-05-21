package downloader

import (
	"fmt"
	//"os"
	"runtime"
	//"runtime/pprof"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
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
}

func NewProgressLogger(intervalInSeconds int) *progressLogger {
	return &progressLogger{
		timer:    time.NewTicker(time.Duration(intervalInSeconds) * time.Second),
		quit:     make(chan struct{}),
		interval: intervalInSeconds,
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
			log.Info("Executed blocks:", "currentBlock", now, "speed (blk/second)", speed,
				"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
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

const StateBatchSize = 128 * 1024 * 1024 // 128 Mb
const ChangeBatchSize = 1024 * 2014      // 1 Mb

func spawnExecuteBlocksStage(stateDB ethdb.Database, blockchain BlockChain) (uint64, error) {
	lastProcessedBlockNumber, err := GetStageProgress(stateDB, Execution)
	if err != nil {
		return 0, err
	}

	nextBlockNumber := uint64(0)

	atomic.StoreUint64(&nextBlockNumber, lastProcessedBlockNumber+1)
	/*
		profileNumber := atomic.LoadUint64(&nextBlockNumber)
		f, err := os.Create(fmt.Sprintf("cpu-%d.prof", profileNumber))
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return lastProcessedBlockNumber, err
		}
		if err1 := pprof.StartCPUProfile(f); err1 != nil {
			log.Error("could not start CPU profile", "error", err1)
			return lastProcessedBlockNumber, err
		}
	*/
	stateBatch := stateDB.NewBatch()
	changeBatch := stateDB.NewBatch()

	progressLogger := NewProgressLogger(logInterval)
	progressLogger.Start(&nextBlockNumber)
	defer progressLogger.Stop()

	accountCache, _ := lru.New(400000)
	storageCache, _ := lru.New(400000)
	//codeCache, _ := lru.New(1000)
	codeSizeCache, _ := lru.New(400000)
	// uncommitedIncarnations map holds incarnations for accounts that were deleted,
	// but their storage is not yet committed
	var uncommitedIncarnations = make(map[common.Address]uint64)

	chainConfig := blockchain.Config()
	engine := blockchain.Engine()
	vmConfig := blockchain.GetVMConfig()
	for {
		blockNum := atomic.LoadUint64(&nextBlockNumber)

		block := blockchain.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}

		var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets

		if core.UsePlainStateExecution {
			plainReader := state.NewPlainStateReaderWithFallback(stateBatch, uncommitedIncarnations)
			plainReader.SetAccountCache(accountCache)
			plainReader.SetStorageCache(storageCache)
			//plainReader.SetCodeCache(codeCache)
			plainReader.SetCodeSizeCache(codeSizeCache)
			stateReader = plainReader
			plainWriter := state.NewPlainStateWriter(stateBatch, changeBatch, blockNum, uncommitedIncarnations)
			plainWriter.SetAccountCache(accountCache)
			plainWriter.SetStorageCache(storageCache)
			//plainWriter.SetCodeCache(codeCache)
			plainWriter.SetCodeSizeCache(codeSizeCache)
			stateWriter = plainWriter
		} else {
			hashStateReader := state.NewDbStateReader(stateBatch, uncommitedIncarnations)
			hashStateReader.SetAccountCache(accountCache)
			hashStateReader.SetStorageCache(storageCache)
			//hashStateReader.SetCodeCache(codeCache)
			hashStateReader.SetCodeSizeCache(codeSizeCache)
			stateReader = hashStateReader
			hashedStateWriter := state.NewDbStateWriter(stateBatch, changeBatch, blockNum, uncommitedIncarnations)
			hashedStateWriter.SetAccountCache(accountCache)
			hashedStateWriter.SetStorageCache(storageCache)
			//hashedStateWriter.SetCodeCache(codeCache)
			hashedStateWriter.SetCodeSizeCache(codeSizeCache)
			stateWriter = hashedStateWriter
		}

		// where the magic happens
		err = core.ExecuteBlockEuphemerally(chainConfig, vmConfig, blockchain, engine, block, stateReader, stateWriter)
		if err != nil {
			return 0, err
		}

		if err = SaveStageProgress(stateBatch, Execution, blockNum); err != nil {
			return 0, err
		}

		atomic.AddUint64(&nextBlockNumber, 1)

		if stateBatch.BatchSize() >= StateBatchSize {
			start := time.Now()
			if _, err = stateBatch.Commit(); err != nil {
				return 0, err
			}
			uncommitedIncarnations = make(map[common.Address]uint64)
			log.Info("State batch committed", "in", time.Since(start))
		}
		if changeBatch.BatchSize() >= ChangeBatchSize {
			if _, err = changeBatch.Commit(); err != nil {
				return 0, err
			}
		}
		/*
			if blockNum-profileNumber == 100000 {
				// Flush the profiler
				pprof.StopCPUProfile()
			}
		*/
	}
	_, err = stateBatch.Commit()
	if err != nil {
		return atomic.LoadUint64(&nextBlockNumber) - 1, fmt.Errorf("sync Execute: failed to write state batch commit: %v", err)
	}
	_, err = changeBatch.Commit()
	if err != nil {
		return atomic.LoadUint64(&nextBlockNumber) - 1, fmt.Errorf("sync Execute: failed to write change batch commit: %v", err)
	}
	return atomic.LoadUint64(&nextBlockNumber) - 1 /* the last processed block */, nil
}

//nolint:unparam
func unwindExecutionStage(unwindPoint uint64, stateDB ethdb.Database) error { //nolint:unparam
	lastProcessedBlockNumber, err := GetStageProgress(stateDB, Execution)
	if err != nil {
		return fmt.Errorf("unwind Execution: get stage progress: %v", err)
	}
	if unwindPoint >= lastProcessedBlockNumber {
		err = SaveStageUnwind(stateDB, Execution, 0)
		if err != nil {
			return fmt.Errorf("unwind Execution: reset: %v", err)
		}
		return nil
	}
	log.Info("Unwind Execution stage", "from", lastProcessedBlockNumber, "to", unwindPoint)
	mutation := stateDB.NewBatch()
	accountMap, storageMap, err2 := stateDB.RewindData(lastProcessedBlockNumber, unwindPoint)
	if err2 != nil {
		return fmt.Errorf("unwind Execution: getting rewind data: %v", err)
	}
	for key, value := range accountMap {
		var addrHash common.Hash
		copy(addrHash[:], []byte(key))
		if len(value) > 0 {
			var acc accounts.Account
			if err = acc.DecodeForStorage(value); err != nil {
				return err
			}
			// Fetch the code hash
			if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
				if codeHash, err2 := stateDB.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash[:], acc.Incarnation)); err2 == nil {
					copy(acc.CodeHash[:], codeHash)
				}
			}
			if err = rawdb.WriteAccount(mutation, addrHash, acc); err != nil {
				return err
			}
		} else {
			if err = rawdb.DeleteAccount(mutation, addrHash); err != nil {
				return err
			}
		}
	}
	for key, value := range storageMap {
		var addrHash common.Hash
		copy(addrHash[:], []byte(key)[:common.HashLength])
		var keyHash common.Hash
		copy(keyHash[:], []byte(key)[common.HashLength+common.IncarnationLength:])
		if len(value) > 0 {
			if err = mutation.Put(dbutils.CurrentStateBucket, []byte(key)[:common.HashLength+common.IncarnationLength+common.HashLength], value); err != nil {
				return err
			}
		} else {
			if err = mutation.Delete(dbutils.CurrentStateBucket, []byte(key)[:common.HashLength+common.IncarnationLength+common.HashLength]); err != nil {
				return err
			}
		}
	}

	for i := lastProcessedBlockNumber; i > unwindPoint; i-- {
		if err = deleteChangeSets(mutation, i); err != nil {
			return err
		}
	}
	err = SaveStageUnwind(mutation, Execution, 0)
	if err != nil {
		return fmt.Errorf("unwind Execution: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind Execute: failed to write db commit: %v", err)
	}
	return nil
}

func deleteChangeSets(batch ethdb.Deleter, timestamp uint64) error {
	changeSetKey := dbutils.EncodeTimestamp(timestamp)
	if err := batch.Delete(dbutils.AccountChangeSetBucket, changeSetKey); err != nil {
		return err
	}
	if err := batch.Delete(dbutils.StorageChangeSetBucket, changeSetKey); err != nil {
		return err
	}
	return nil
}
