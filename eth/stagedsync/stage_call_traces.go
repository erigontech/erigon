package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

type CallTracesCfg struct {
	ToBlock     uint64 // not setting this params means no limit
	BatchSize   datasize.ByteSize
	tmpdir      string
	chainConfig *params.ChainConfig
	engine      consensus.Engine
}

func StageCallTracesCfg(
	ToBlock uint64,
	BatchSize datasize.ByteSize,
	tmpdir string,
	chainConfig *params.ChainConfig,
	engine consensus.Engine,
) CallTracesCfg {
	return CallTracesCfg{
		ToBlock:     ToBlock,
		BatchSize:   BatchSize,
		tmpdir:      tmpdir,
		chainConfig: chainConfig,
		engine:      engine,
	}
}

func SpawnCallTraces(s *StageState, db ethdb.Database, quit <-chan struct{}, cfg CallTracesCfg) error {
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
	if cfg.ToBlock > 0 && cfg.ToBlock < endBlock {
		endBlock = cfg.ToBlock
	}
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	if err := promoteCallTraces(logPrefix, tx, s.BlockNumber+1, endBlock, bitmapsBufLimit, bitmapsFlushEvery, quit, cfg); err != nil {
		return err
	}

	if err := s.DoneAndUpdate(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func promoteCallTraces(logPrefix string, tx ethdb.Database, startBlock, endBlock uint64, bufLimit datasize.ByteSize, flushEvery time.Duration, quit <-chan struct{}, cfg CallTracesCfg) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]*roaring.Bitmap{}
	tos := map[string]*roaring.Bitmap{}
	collectorFrom := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	collectorTo := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	checkFlushEvery := time.NewTicker(flushEvery)
	defer checkFlushEvery.Stop()

	prev := startBlock
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		select {
		default:
		case <-logEvery.C:
			sz, err := tx.(ethdb.HasTx).Tx().(ethdb.HasStats).BucketSize(dbutils.CallFromIndex)
			if err != nil {
				return err
			}
			sz2, err := tx.(ethdb.HasTx).Tx().(ethdb.HasStats).BucketSize(dbutils.CallToIndex)
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
			if needFlush(froms, bufLimit) {
				if err := flushBitmaps(collectorFrom, froms); err != nil {
					return fmt.Errorf("[%s] %w", logPrefix, err)
				}

				froms = map[string]*roaring.Bitmap{}
			}

			if needFlush(tos, bufLimit) {
				if err := flushBitmaps(collectorTo, tos); err != nil {
					return fmt.Errorf("[%s] %w", logPrefix, err)
				}

				tos = map[string]*roaring.Bitmap{}
			}
		}
		blockHash, err2 := rawdb.ReadCanonicalHash(tx, blockNum)
		if err2 != nil {
			return fmt.Errorf("%s: getting canonical blockhadh for block %d: %v", logPrefix, blockNum, err2)
		}
		block, _, err := rawdb.ReadBlockWithSenders(tx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			break
		}

		stateReader := state.NewPlainDBState(tx, blockNum-1)
		stateWriter := state.NewNoopWriter()
		tracer := NewCallTracer()
		vmConfig := &vm.Config{Debug: true, NoReceipts: true, ReadOnly: false, Tracer: tracer}
		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
		if _, err := core.ExecuteBlockEphemerally(cfg.chainConfig, vmConfig, getHeader, cfg.engine, block, stateReader, stateWriter); err != nil {
			return fmt.Errorf("[%s] %w", logPrefix, err)
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
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}
	if err := flushBitmaps(collectorTo, tos); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	var currentBitmap = roaring.New()
	var buf = bytes.NewBuffer(nil)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		lastChunkKey := make([]byte, len(k)+4)
		copy(lastChunkKey, k)
		binary.BigEndian.PutUint32(lastChunkKey[len(k):], ^uint32(0))
		lastChunkBytes, err := table.Get(lastChunkKey)
		if err != nil {
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
		if err := bitmapdb.WalkChunkWithKeys(k, currentBitmap, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring.Bitmap) error {
			buf.Reset()
			if _, err := chunk.WriteTo(buf); err != nil {
				return err
			}
			return next(k, chunkKey, buf.Bytes())
		}); err != nil {
			return err
		}
		currentBitmap.Clear()
		return nil
	}

	if err := collectorFrom.Load(logPrefix, tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.CallFromIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := collectorTo.Load(logPrefix, tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.CallToIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}
	return nil
}

func UnwindCallTraces(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}, cfg CallTracesCfg) error {
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
	if err := unwindCallTraces(logPrefix, tx, s.BlockNumber, u.UnwindPoint, quitCh, cfg); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: %w", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("[%s] %w", logPrefix, err)
		}
	}

	return nil
}

func unwindCallTraces(logPrefix string, db ethdb.Database, from, to uint64, quitCh <-chan struct{}, cfg CallTracesCfg) error {
	froms := map[string]struct{}{}
	tos := map[string]struct{}{}

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
		block, _, err := rawdb.ReadBlockWithSenders(db, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			break
		}

		stateReader := state.NewPlainDBState(db, blockNum-1)
		stateWriter := state.NewNoopWriter()
		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(db, hash, number) }
		if _, err = core.ExecuteBlockEphemerally(cfg.chainConfig, vmConfig, getHeader, cfg.engine, block, stateReader, stateWriter); err != nil {
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

	if err := truncateBitmaps(db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.CallFromIndex, froms, to); err != nil {
		return err
	}
	if err := truncateBitmaps(db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.CallToIndex, tos, to); err != nil {
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

func (ct *CallTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	//TODO: Populate froms and tos if it is any call opcode
	return nil
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct *CallTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
