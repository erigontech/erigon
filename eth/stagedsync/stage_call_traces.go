package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"

	"github.com/RoaringBitmap/gocroaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
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
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	froms := map[string]*gocroaring.Bitmap{}
	tos := map[string]*gocroaring.Bitmap{}
	callFromIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.CallFromIndex)
	defer callFromIndexCursor.Close()
	callToIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.CallToIndex)
	defer callToIndexCursor.Close()
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
		accountCache = fastcache.New(512 * 1024 * 1024) // 512 Mb
		storageCache = fastcache.New(512 * 1024 * 1024) // 512 Mb
		codeCache = fastcache.New(32 * 1024 * 1024)     // 32 Mb (the minimum)
		codeSizeCache = fastcache.New(32 * 1024 * 1024) // 32 Mb (the minimum)
	}

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
			log.Info("Progress", "blockNum", blockNum, dbutils.CallFromIndex, common.StorageSize(sz), dbutils.CallToIndex, common.StorageSize(sz2))
		case <-checkFlushEvery.C:
			if needFlush(froms, callIndicesMemLimit, bitmapdb.HotShardLimit/2) {
				if err := flushBitmaps(callFromIndexCursor, froms); err != nil {
					return err
				}

				froms = map[string]*gocroaring.Bitmap{}
			}

			if needFlush(tos, callIndicesMemLimit, bitmapdb.HotShardLimit/2) {
				if err := flushBitmaps(callToIndexCursor, tos); err != nil {
					return err
				}

				tos = map[string]*gocroaring.Bitmap{}
			}
		}

		blockHash := rawdb.ReadCanonicalHash(tx, blockNum)
		block := rawdb.ReadBlock(tx, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(tx, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

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
		vmConfig := &vm.Config{Debug: true, Tracer: tracer}
		_, err := core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter)
		if err != nil {
			return err
		}
		for addr := range tracer.froms {
			m, ok := froms[string(addr[:])]
			if !ok {
				m = gocroaring.New()
				a := addr // To copy addr
				froms[string(a[:])] = m
			}
			m.Add(uint32(blockNum))
		}
		for addr := range tracer.tos {
			m, ok := tos[string(addr[:])]
			if !ok {
				m = gocroaring.New()
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

func unwindCallTraces(tx rawdb.DatabaseReader, from, to uint64, chainConfig *params.ChainConfig, chainContext core.ChainContext, quitCh <-chan struct{}) error {
	froms := map[string]bool{}
	tos := map[string]bool{}
	fromIndex := tx.(ethdb.HasTx).Tx().Cursor(dbutils.CallFromIndex)
	toIndex := tx.(ethdb.HasTx).Tx().Cursor(dbutils.CallToIndex)
	engine := chainContext.Engine()

	tracer := NewCallTracer()
	vmConfig := &vm.Config{Debug: true, Tracer: tracer}
	for blockNum := to + 1; blockNum <= from; blockNum++ {
		if err := common.Stopped(quitCh); err != nil {
			return err
		}

		blockHash := rawdb.ReadCanonicalHash(tx, blockNum)
		block := rawdb.ReadBlock(tx, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(tx, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

		var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets

		stateReader = state.NewPlainDBState(tx.(ethdb.HasTx).Tx(), blockNum-1)
		stateWriter = state.NewCacheStateWriter()

		_, err := core.ExecuteBlockEphemerally(chainConfig, vmConfig, chainContext, engine, block, stateReader, stateWriter)
		if err != nil {
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

	if err := truncateBitmaps(fromIndex, froms, to+1, from+1); err != nil {
		return err
	}
	if err := truncateBitmaps(toIndex, tos, to+1, from+1); err != nil {
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
