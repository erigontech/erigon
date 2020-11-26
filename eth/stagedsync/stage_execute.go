package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
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
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
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
	CacheSize     int
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

	var batch ethdb.DbWithPendingMutations
	var cache *shards.StateCache
	if params.CacheSize == 0 {
		batch = tx.NewBatch()
		defer batch.Rollback()
	} else {
		batch = tx
		cache = shards.NewStateCache(32, params.CacheSize)
	}

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
		if cache != nil {
			stateReader = state.NewCachedReader(stateReader, cache)
		}

		if params.WriterBuilder != nil {
			stateWriter = params.WriterBuilder(batch, tx, blockNum)
		} else {
			stateWriter = state.NewPlainStateWriter(batch, tx, blockNum)
		}
		stateWriter = state.NewCachedWriter(stateWriter, cache)

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

		if cache == nil {
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
		} else {
			if cache.WriteSize() >= params.BatchSize {
				if err = s.Update(tx, blockNum); err != nil {
					return err
				}
				start := time.Now()
				writes := cache.PrepareWrites()
				log.Info("PrepareWrites", "in", time.Since(start))
				if err = commitCache(tx, writes); err != nil {
					return err
				}
				if !useExternalTx {
					if err = tx.CommitAndBegin(context.Background()); err != nil {
						return err
					}
					chainContext.SetDB(tx)
				}
				start = time.Now()
				cache.TurnWritesToReads(writes)
				log.Info("TurnWritesToReads", "in", time.Since(start))
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
			logBlock = logProgress(logPrefix, logBlock, blockNum, batch, cache)
		}
	}

	logProgress(logPrefix, logBlock, stageProgress, batch, cache)
	if cache == nil {
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
	} else {
		if err := s.Update(tx, stageProgress); err != nil {
			return err
		}
		writes := cache.PrepareWrites()
		if err := commitCache(tx, writes); err != nil {
			return err
		}
		if !useExternalTx {
			if _, err := tx.Commit(); err != nil {
				return err
			}
		}
		cache.TurnWritesToReads(writes)
	}
	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	s.Done()
	return nil
}

func commitCache(tx ethdb.DbWithPendingMutations, writes *btree.BTree) error {
	return shards.WalkWrites(writes,
		func(address []byte, account *accounts.Account) error { // accountWrite
			value := make([]byte, account.EncodingLengthForStorage())
			account.EncodeForStorage(value)
			return tx.Put(dbutils.PlainStateBucket, address, value)
		},
		func(address []byte, original *accounts.Account) error { // accountDelete
			if err := tx.Delete(dbutils.PlainStateBucket, address[:], nil); err != nil {
				return err
			}
			if original != nil && original.Incarnation > 0 {
				var b [8]byte
				binary.BigEndian.PutUint64(b[:], original.Incarnation)
				if err := tx.Put(dbutils.IncarnationMapBucket, address, b[:]); err != nil {
					return err
				}
			}
			return nil
		},
		func(address []byte, incarnation uint64, location []byte, value []byte) error { // storageWrite
			compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, location)
			return tx.Put(dbutils.PlainStateBucket, compositeKey, value)
		},
		func(address []byte, incarnation uint64, location []byte) error { // storageDelete
			compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, location)
			return tx.Delete(dbutils.PlainStateBucket, compositeKey, nil)
		},
		func(address []byte, incarnation uint64, code []byte) error { // codeWrite
			h := common.NewHasher()
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(code)
			var codeHash common.Hash
			//nolint:errcheck
			h.Sha.Read(codeHash[:])
			if err := tx.Put(dbutils.CodeBucket, codeHash.Bytes(), code); err != nil {
				return err
			}
			return tx.Put(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address, incarnation), codeHash.Bytes())
		},
		func(address []byte, incarnation uint64) error { // codeDelete
			return nil
		},
	)
}

func logProgress(logPrefix string, prev, now uint64, batch ethdb.DbWithPendingMutations, cache *shards.StateCache) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if cache == nil {
		log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix),
			"number", now,
			"blk/second", speed,
			"batch", common.StorageSize(batch.BatchSize()),
			"alloc", common.StorageSize(m.Alloc),
			"sys", common.StorageSize(m.Sys),
			"numGC", int(m.NumGC))
	} else {
		log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix),
			"number", now,
			"blk/second", speed,
			"cache writes", common.StorageSize(cache.WriteSize()), "cache read", common.StorageSize(cache.ReadSize()),
			"alloc", common.StorageSize(m.Alloc),
			"sys", common.StorageSize(m.Sys),
			"numGC", int(m.NumGC))
	}
	return now
}

func UnwindExecutionStage(u *UnwindState, s *StageState, stateDB ethdb.Database, writeReceipts bool) error {
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

	rewindFunc := changeset.RewindDataPlain
	stateBucket := dbutils.PlainStateBucket
	storageKeyLength := common.AddressLength + common.IncarnationLength + common.HashLength
	deleteAccountFunc := deleteAccountPlain
	writeAccountFunc := writeAccountPlain
	recoverCodeHashFunc := recoverCodeHashPlain

	accountMap, storageMap, errRewind := rewindFunc(tx, s.BlockNumber, u.UnwindPoint)
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
			recoverCodeHashFunc(&acc, tx, key)
			if err := writeAccountFunc(logPrefix, tx, key, acc); err != nil {
				return err
			}
		} else {
			if err := deleteAccountFunc(tx, key); err != nil {
				return err
			}
		}
	}

	for key, value := range storageMap {
		if len(value) > 0 {
			if err := tx.Put(stateBucket, []byte(key)[:storageKeyLength], value); err != nil {
				return err
			}
		} else {
			if err := tx.Delete(stateBucket, []byte(key)[:storageKeyLength], nil); err != nil {
				return err
			}
		}
	}

	if err := changeset.Truncate(tx.(ethdb.HasTx).Tx(), u.UnwindPoint+1); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if writeReceipts {
		if err := rawdb.DeleteNewerReceipts(tx, u.UnwindPoint+1); err != nil {
			return fmt.Errorf("%s: walking receipts: %v", logPrefix, err)
		}
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
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

func deleteAccountPlain(db ethdb.Deleter, key string) error {
	var address common.Address
	copy(address[:], []byte(key))
	return rawdb.PlainDeleteAccount(db, address)
}

func deleteChangeSets(batch ethdb.Deleter, timestamp uint64, accountBucket, storageBucket string) error {
	changeSetKey := dbutils.EncodeBlockNumber(timestamp)
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
