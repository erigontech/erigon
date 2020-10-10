package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"

	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const (
	callIndicesMemLimit       = 512 * datasize.MB
	callIndicesCheckSizeEvery = 10 * time.Second
)

func SpawnCallTraces(s *StageState, db ethdb.Database, chainConfig *params.ChainConfig, chainContext core.ChainContext, datadir string, quit <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("call traces: getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	if err := promoteCallTraces(tx, s.BlockNumber+1, endBlock, chainConfig, chainContext, quit); err != nil {
		return err
	}

	if err := s.DoneAndUpdate(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func promoteCallTraces(tx rawdb.DatabaseReader, startBlock, endBlock uint64, chainConfig *params.ChainConfig, chainContext core.ChainContext, quit <-chan struct{}) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]*roaring.Bitmap{}
	tos := map[string]*roaring.Bitmap{}
	callFromIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.CallFromIndex)
	defer callFromIndexCursor.Close()
	callToIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.CallToIndex)
	defer callToIndexCursor.Close()
	accountChangesCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.PlainAccountChangeSetBucket)
	defer accountChangesCursor.Close()
	storageChangesCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.PlainStorageChangeSetBucket)
	checkFlushEvery := time.NewTicker(callIndicesCheckSizeEvery)
	defer checkFlushEvery.Stop()
	engine := chainContext.Engine()

	var caching bool = endBlock-startBlock > 100
	var accountCache *fastcache.Cache
	var storageCache *fastcache.Cache
	var codeCache *fastcache.Cache
	var codeSizeCache *fastcache.Cache
	// Caching is not worth it for small runs of blocks
	if caching {
		// Caching is not worth it for small runs of blocks
		accountCache = fastcache.New(2 * 1024 * 1024 * 1024) // 2 Gb
		storageCache = fastcache.New(2 * 1024 * 1024 * 1024) // 2 Gb
		codeCache = fastcache.New(512 * 1024 * 1024)         // 512 Mb
		codeSizeCache = fastcache.New(32 * 1024 * 1024)      // 32 Mb (the minimum)
	}

	prev := startBlock
	accountCsKey, accountCsVal, errAcc := accountChangesCursor.Seek(dbutils.EncodeTimestamp(startBlock))
	if errAcc != nil {
		return fmt.Errorf("seeking in account changeset cursor: %v", errAcc)
	}
	accountsPreset := 0
	storageCsKey, storageCsVal, errSt := storageChangesCursor.Seek(dbutils.EncodeTimestamp(startBlock))
	if errSt != nil {
		return fmt.Errorf("seeking in storage changeset cursor: %v", errSt)
	}
	storagePreset := 0
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		select {
		default:
		case <-logEvery.C:
			sz, err := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.CallFromIndex)
			if err != nil {
				return err
			}
			sz2, err := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.CallToIndex)
			if err != nil {
				return err
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			speed := float64(blockNum-prev) / float64(logInterval/time.Second)
			prev = blockNum

			log.Info("Progress", "blockNum", blockNum, dbutils.CallFromIndex, common.StorageSize(sz), dbutils.CallToIndex, common.StorageSize(sz2),
				"blk/second", speed,
				"accounts preset", accountsPreset,
				"storage preset", storagePreset,
				"alloc", common.StorageSize(m.Alloc),
				"sys", common.StorageSize(m.Sys),
				"numGC", int(m.NumGC))
			accountsPreset = 0
			storagePreset = 0
		case <-checkFlushEvery.C:
			if needFlush(froms, callIndicesMemLimit) {
				if err := flushBitmaps(callFromIndexCursor, froms); err != nil {
					return err
				}

				froms = map[string]*roaring.Bitmap{}
			}

			if needFlush(tos, callIndicesMemLimit) {
				if err := flushBitmaps(callToIndexCursor, tos); err != nil {
					return err
				}

				tos = map[string]*roaring.Bitmap{}
			}
		}
		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return fmt.Errorf("getting canonical blockhadh for block %d: %v", blockNum, err)
		}
		block := rawdb.ReadBlock(tx, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(tx, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

		if accountCsKey != nil {
			accountCsBlockNum, _ := dbutils.DecodeTimestamp(accountCsKey)
			if accountCsBlockNum == blockNum {
				cs := changeset.AccountChangeSetPlainBytes(accountCsVal)
				accountCsKey, accountCsVal, errAcc = accountChangesCursor.Next()
				if errAcc != nil {
					return fmt.Errorf("seeking in account changeset cursor: %v", errAcc)
				}
				if errAcc = cs.Walk(func(k, v []byte) error {
					if len(v) == 0 {
						accountCache.Set(k, nil)
					} else {
						accountCache.Set(k, v)
					}
					accountsPreset++
					return nil
				}); errAcc != nil {
					return fmt.Errorf("walking in account changeset: %v", errAcc)
				}
			}
		}
		if storageCsKey != nil {
			storageCsBlockNum, _ := dbutils.DecodeTimestamp(storageCsKey)
			if storageCsBlockNum == blockNum {
				cs := changeset.StorageChangeSetPlainBytes(storageCsVal)
				storageCsKey, storageCsVal, errSt = storageChangesCursor.Next()
				if errSt != nil {
					return fmt.Errorf("seeking in storage changeset cursor: %v", errSt)
				}
				if errSt = cs.Walk(func(k, v []byte) error {
					if len(v) == 0 {
						storageCache.Set(k, nil)
					} else {
						storageCache.Set(k, v)
					}
					storagePreset++
					return nil
				}); errSt != nil {
					return fmt.Errorf("walking in storage changeset: %v", errSt)
				}
			}
		}
		stateReader := state.NewPlainDBState(tx.(ethdb.HasTx).Tx(), blockNum-1)
		stateWriter := state.NewCacheStateWriter()

		if caching {
			stateReader.SetAccountCache(accountCache)
			stateReader.SetStorageCache(storageCache)
			stateReader.SetCodeCache(codeCache)
			stateReader.SetCodeSizeCache(codeSizeCache)
			stateWriter.SetAccountCache(accountCache)
			stateWriter.SetStorageCache(storageCache)
			stateWriter.SetCodeCache(codeCache)
			stateWriter.SetCodeSizeCache(codeSizeCache)
		}

		tracer := NewCallTracer()
		vmConfig := &vm.Config{Debug: true, NoReceipts: true, ReadOnly: false, Tracer: tracer}
		if _, err = core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter); err != nil {
			return err
		}
		for addr := range tracer.froms {
			m, ok := froms[string(addr[:])]
			if !ok {
				m = roaring.New()
				a := addr // To copy addr
				froms[string(a[:])] = m
			}
			m.Add(uint32(blockNum))
		}
		for addr := range tracer.tos {
			m, ok := tos[string(addr[:])]
			if !ok {
				m = roaring.New()
				a := addr // To copy addr
				tos[string(a[:])] = m
			}
			m.Add(uint32(blockNum))
		}
	}

	if err := flushBitmaps(callFromIndexCursor, froms); err != nil {
		return err
	}
	if err := flushBitmaps(callToIndexCursor, tos); err != nil {
		return err
	}
	return nil
}

func UnwindCallTraces(u *UnwindState, s *StageState, db ethdb.Database, chainConfig *params.ChainConfig, chainContext core.ChainContext, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := unwindCallTraces(tx, s.BlockNumber, u.UnwindPoint, chainConfig, chainContext, quitCh); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("unwind CallTraces: %w", err)
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func unwindCallTraces(db rawdb.DatabaseReader, from, to uint64, chainConfig *params.ChainConfig, chainContext core.ChainContext, quitCh <-chan struct{}) error {
	froms := map[string]bool{}
	tos := map[string]bool{}
	tx := db.(ethdb.HasTx).Tx()
	engine := chainContext.Engine()

	tracer := NewCallTracer()
	vmConfig := &vm.Config{Debug: true, NoReceipts: true, Tracer: tracer}
	for blockNum := to + 1; blockNum <= from; blockNum++ {
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		blockHash, err := rawdb.ReadCanonicalHash(db, blockNum)
		if err != nil {
			return fmt.Errorf("getting canonical blockhadh for block %d: %v", blockNum, err)
		}
		block := rawdb.ReadBlock(db, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(db, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

		var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets

		stateReader = state.NewPlainDBState(tx, blockNum-1)
		stateWriter = state.NewCacheStateWriter()

		if _, err = core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter); err != nil {
			return err
		}
	}
	for addr := range tracer.froms {
		a := addr // To copy addr
		froms[string(a[:])] = true
	}
	for addr := range tracer.tos {
		a := addr // To copy addr
		tos[string(a[:])] = true
	}

	if err := truncateBitmaps(tx, dbutils.CallFromIndex, froms, to+1, from+1); err != nil {
		return err
	}
	if err := truncateBitmaps(tx, dbutils.CallToIndex, tos, to+1, from+1); err != nil {
		return err
	}
	return nil
}

type CallTracer struct {
	froms map[common.Address]struct{}
	tos   map[common.Address]struct{}
}

func NewCallTracer() *CallTracer {
	return &CallTracer{
		froms: make(map[common.Address]struct{}),
		tos:   make(map[common.Address]struct{}),
	}
}

func (ct *CallTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {
	//TODO: Populate froms and tos if it is any call opcode
	return nil
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct *CallTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
