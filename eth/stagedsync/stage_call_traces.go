package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

const (
	callIndicesMemLimit       = 256 * datasize.MB
	callIndicesCheckSizeEvery = 30 * time.Second
)

type StateAccessBuilder func(db ethdb.Database, blockNumber uint64,
	accountCache, storageCache, codeCache, codeSizeCache *fastcache.Cache) (state.StateReader, state.WriterWithChangeSets)

type CallTracesStageParams struct {
	ToBlock       uint64 // not setting this params means no limit
	AccessBuilder StateAccessBuilder
	PresetChanges bool // Whether to use changesets to pre-set values in the cache
}

func SpawnCallTraces(s *StageState, db ethdb.Database, chainConfig *params.ChainConfig, chainContext core.ChainContext, tmpdir string, quit <-chan struct{}, params CallTracesStageParams) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	if params.ToBlock > 0 && params.ToBlock < endBlock {
		endBlock = params.ToBlock
	}
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	if err := promoteCallTraces(logPrefix, tx, s.BlockNumber+1, endBlock, chainConfig, chainContext, tmpdir, quit, params); err != nil {
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

func promoteCallTraces(logPrefix string, tx ethdb.Database, startBlock, endBlock uint64, chainConfig *params.ChainConfig, chainContext core.ChainContext, tmpdir string, quit <-chan struct{}, params CallTracesStageParams) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]*roaring.Bitmap{}
	tos := map[string]*roaring.Bitmap{}
	collectorFrom := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	collectorTo := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	accountChangesCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.PlainAccountChangeSetBucket2)
	defer accountChangesCursor.Close()
	storageChangesCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.PlainStorageChangeSetBucket2)
	checkFlushEvery := time.NewTicker(callIndicesCheckSizeEvery)
	defer checkFlushEvery.Stop()
	engine := chainContext.Engine()

	var caching = endBlock-startBlock > 100
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
	var accountCsKey, accountCsVal []byte
	var errAcc error
	var storageCsKey, storageCsVal []byte
	var errSt error
	if params.PresetChanges {
		accountCsKey, accountCsVal, errAcc = accountChangesCursor.Seek(dbutils.EncodeTimestamp(startBlock))
		if errAcc != nil {
			return fmt.Errorf("%s: seeking in account changeset cursor: %v", logPrefix, errAcc)
		}
		storageCsKey, storageCsVal, errSt = storageChangesCursor.Seek(dbutils.EncodeTimestamp(startBlock))
		if errSt != nil {
			return fmt.Errorf("%s: seeking in storage changeset cursor: %v", logPrefix, errSt)
		}
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
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			speed := float64(blockNum-prev) / float64(logInterval/time.Second)
			prev = blockNum

			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum, dbutils.CallFromIndex, common.StorageSize(sz), dbutils.CallToIndex, common.StorageSize(sz2),
				"blk/second", speed,
				"alloc", common.StorageSize(m.Alloc),
				"sys", common.StorageSize(m.Sys),
				"numGC", int(m.NumGC))
		case <-checkFlushEvery.C:
			if needFlush(froms, callIndicesMemLimit) {
				if err := flushBitmaps(collectorFrom, froms); err != nil {
					return err
				}

				froms = map[string]*roaring.Bitmap{}
			}

			if needFlush(tos, callIndicesMemLimit) {
				if err := flushBitmaps(collectorTo, tos); err != nil {
					return err
				}

				tos = map[string]*roaring.Bitmap{}
			}
		}
		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return fmt.Errorf("%s: getting canonical blockhadh for block %d: %v", logPrefix, blockNum, err)
		}
		block := rawdb.ReadBlock(tx, blockHash, blockNum)
		if block == nil {
			break
		}
		senders := rawdb.ReadSenders(tx, blockHash, blockNum)
		block.Body().SendersToTxs(senders)

		var stateReader state.StateReader
		var stateWriter state.WriterWithChangeSets
		if params.AccessBuilder != nil {
			reader, writer := params.AccessBuilder(tx, blockNum-1, accountCache, storageCache, codeCache, codeSizeCache)
			stateReader = reader
			stateWriter = writer
		} else {
			reader := state.NewPlainDBState(tx.(ethdb.HasTx).Tx(), blockNum-1)
			writer := state.NewCacheStateWriter()
			if caching {
				reader.SetAccountCache(accountCache)
				reader.SetStorageCache(storageCache)
				reader.SetCodeCache(codeCache)
				reader.SetCodeSizeCache(codeSizeCache)
				writer.SetAccountCache(accountCache)
				writer.SetStorageCache(storageCache)
				writer.SetCodeCache(codeCache)
				writer.SetCodeSizeCache(codeSizeCache)
			}
			stateReader = reader
			stateWriter = writer
		}
		if params.PresetChanges && accountCsKey != nil {
			accountCsBlockNum, _ := dbutils.DecodeTimestamp(accountCsKey)
			if accountCsBlockNum == blockNum {
				cs := changeset.AccountChangeSetPlainBytes(accountCsVal)
				accountCsKey, accountCsVal, errAcc = accountChangesCursor.Next()
				if errAcc != nil {
					return fmt.Errorf("%s: seeking in account changeset cursor: %v", logPrefix, errAcc)
				}
				if errAcc = cs.Walk(func(k, v []byte) error {
					if len(v) == 0 {
						accountCache.Set(k, nil)
					} else {
						accountCache.Set(k, v)
					}
					return nil
				}); errAcc != nil {
					return fmt.Errorf("%s: walking in account changeset: %v", logPrefix, errAcc)
				}
			}
		}
		if params.PresetChanges && storageCsKey != nil {
			storageCsBlockNum, _ := dbutils.DecodeTimestamp(storageCsKey)
			if storageCsBlockNum == blockNum {
				cs := changeset.StorageChangeSetPlainBytes(storageCsVal)
				storageCsKey, storageCsVal, errSt = storageChangesCursor.Next()
				if errSt != nil {
					return fmt.Errorf("%s: seeking in storage changeset cursor: %v", logPrefix, errSt)
				}
				if errSt = cs.Walk(func(k, v []byte) error {
					if len(v) == 0 {
						storageCache.Set(k, nil)
					} else {
						storageCache.Set(k, v)
					}
					return nil
				}); errSt != nil {
					return fmt.Errorf("%s: walking in storage changeset: %v", logPrefix, errSt)
				}
			}
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

	if err := flushBitmaps(collectorFrom, froms); err != nil {
		return err
	}
	if err := flushBitmaps(collectorTo, tos); err != nil {
		return err
	}

	var currentBitmap = roaring.New()
	var buf = bytes.NewBuffer(nil)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		lastChunkKey := make([]byte, len(k)+4)
		copy(lastChunkKey, k)
		binary.BigEndian.PutUint32(lastChunkKey[len(k):], ^uint32(0))
		lastChunkBytes, err := table.Get(lastChunkKey)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("%s: find last chunk failed: %w", logPrefix, err)
		}

		lastChunk := roaring.New()
		if len(lastChunkBytes) > 0 {
			_, err = lastChunk.FromBuffer(lastChunkBytes)
			if err != nil {
				return fmt.Errorf("%s: couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", logPrefix, err, len(lastChunkBytes))
			}
		}

		if _, err := currentBitmap.FromBuffer(v); err != nil {
			return err
		}
		currentBitmap.Or(lastChunk) // merge last existing chunk from db - next loop will overwrite it
		nextChunk := bitmapdb.ChunkIterator(currentBitmap, bitmapdb.ChunkLimit)
		for chunk := nextChunk(); chunk != nil; chunk = nextChunk() {
			buf.Reset()
			if _, err := chunk.WriteTo(buf); err != nil {
				return err
			}
			chunkKey := make([]byte, len(k)+4)
			copy(chunkKey, k)
			if currentBitmap.GetCardinality() == 0 {
				binary.BigEndian.PutUint32(chunkKey[len(k):], ^uint32(0))
				if err := next(k, chunkKey, common.CopyBytes(buf.Bytes())); err != nil {
					return err
				}
				break
			}
			binary.BigEndian.PutUint32(chunkKey[len(k):], chunk.Maximum())
			if err := next(k, chunkKey, common.CopyBytes(buf.Bytes())); err != nil {
				return err
			}
		}

		currentBitmap.Clear()
		return nil
	}

	if err := collectorFrom.Load(logPrefix, tx, dbutils.CallFromIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	if err := collectorTo.Load(logPrefix, tx, dbutils.CallToIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
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
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.state.LogPrefix()
	if err := unwindCallTraces(logPrefix, tx, s.BlockNumber, u.UnwindPoint, chainConfig, chainContext, quitCh); err != nil {
		return fmt.Errorf("unwindCallTraces fail: %w", err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: %w", logPrefix, err)
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func unwindCallTraces(logPrefix string, db rawdb.DatabaseReader, from, to uint64, chainConfig *params.ChainConfig, chainContext core.ChainContext, quitCh <-chan struct{}) error {
	froms := map[string]struct{}{}
	tos := map[string]struct{}{}
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
			return fmt.Errorf("%s: getting canonical blockhadh for block %d: %v", logPrefix, blockNum, err)
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
			return fmt.Errorf("exec block: %w", err)
		}
	}
	for addr := range tracer.froms {
		a := addr // To copy addr
		froms[string(a[:])] = struct{}{}
	}
	for addr := range tracer.tos {
		a := addr // To copy addr
		tos[string(a[:])] = struct{}{}
	}

	if err := truncateBitmaps(db.(ethdb.HasTx).Tx(), dbutils.CallFromIndex, froms, to+1, from+1); err != nil {
		return err
	}
	if err := truncateBitmaps(db.(ethdb.HasTx).Tx(), dbutils.CallToIndex, tos, to+1, from+1); err != nil {
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
