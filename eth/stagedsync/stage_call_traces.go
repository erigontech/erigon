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
	"github.com/RoaringBitmap/roaring/roaring64"
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
	db          ethdb.RwKV
	ToBlock     uint64 // not setting this params means no limit
	BatchSize   datasize.ByteSize
	tmpdir      string
	chainConfig *params.ChainConfig
	engine      consensus.Engine
}

func StageCallTracesCfg(
	db ethdb.RwKV,
	ToBlock uint64,
	BatchSize datasize.ByteSize,
	tmpdir string,
	chainConfig *params.ChainConfig,
	engine consensus.Engine,
) CallTracesCfg {
	return CallTracesCfg{
		db:          db,
		ToBlock:     ToBlock,
		BatchSize:   BatchSize,
		tmpdir:      tmpdir,
		chainConfig: chainConfig,
		engine:      engine,
	}
}

func SpawnCallTraces(s *StageState, tx ethdb.RwTx, quit <-chan struct{}, cfg CallTracesCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
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

func promoteCallTraces(logPrefix string, tx ethdb.RwTx, startBlock, endBlock uint64, bufLimit datasize.ByteSize, flushEvery time.Duration, quit <-chan struct{}, cfg CallTracesCfg) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]*roaring.Bitmap{}
	tos := map[string]*roaring.Bitmap{}
	collectorFrom := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	collectorTo := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	checkFlushEvery := time.NewTicker(flushEvery)
	defer checkFlushEvery.Stop()

	traceCursor, err := tx.RoCursorDupSort(dbutils.CallTraceSet)
	if err != nil {
		return fmt.Errorf("%s: failed to create cursor for call traces: %v", logPrefix, err)
	}

	if err := ethdb.Walk(tx, dbutils.CallTraceSet, dbutils.EncodeBlockNumber(startBlock), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if blockN >= stop {
			return false, nil
		}
		if err := common.Stopped(quit); err != nil {
			return false, err
		}

		k = dbutils.CompositeKeyWithoutIncarnation(k)

		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockN, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		case <-checkFlushEvery.C:
			if needFlush64(updates, cfg.bufLimit) {
				if err := flushBitmaps64(collectorUpdates, updates); err != nil {
					return false, err
				}
				updates = map[string]*roaring64.Bitmap{}
			}
		}

		kStr := string(k)
		m, ok := updates[kStr]
		if !ok {
			m = roaring64.New()
			updates[kStr] = m
		}
		m.Add(blockN)

		return true, nil
	}); err != nil {
		return err
	}

	if err := flushBitmaps64(collectorUpdates, updates); err != nil {
		return err
	}

	prev := startBlock
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			speed := float64(blockNum-prev) / float64(logInterval/time.Second)
			prev = blockNum

			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum,
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
			return fmt.Errorf("no block: %d", blockNum)
		}

		stateReader := state.NewPlainKvState(tx, blockNum-1)
		stateWriter := state.NewNoopWriter()
		tracer := NewCallTracer()
		vmConfig := &vm.Config{Debug: true, NoReceipts: true, ReadOnly: false, Tracer: tracer}
		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
		if _, err := core.ExecuteBlockEphemerally(cfg.chainConfig, vmConfig, getHeader, cfg.engine, block, stateReader, stateWriter); err != nil {
			return fmt.Errorf("[%s] %w", logPrefix, err)
		}

		tracer.tos[block.Coinbase()] = struct{}{}
		for _, uncle := range block.Uncles() {
			tracer.tos[uncle.Coinbase] = struct{}{}
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
	if err := finaliseCallTraces(froms, tos, collectorFrom, collectorTo, logPrefix, tx, quit); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}
	return nil
}

func finaliseCallTraces(froms, tos map[string]*roaring.Bitmap, collectorFrom, collectorTo *etl.Collector, logPrefix string, tx ethdb.RwTx, quit <-chan struct{}) error {
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
		if err != nil {
			return fmt.Errorf("find last chunk failed: %w", err)
		}

		lastChunk := roaring.New()
		if len(lastChunkBytes) > 0 {
			_, err = lastChunk.FromBuffer(lastChunkBytes)
			if err != nil {
				return fmt.Errorf("couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", err, len(lastChunkBytes))
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

	if err := collectorFrom.Load(logPrefix, tx, dbutils.CallFromIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	if err := collectorTo.Load(logPrefix, tx, dbutils.CallToIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func UnwindCallTraces(u *UnwindState, s *StageState, tx ethdb.RwTx, quitCh <-chan struct{}, cfg CallTracesCfg) error {
	if s.BlockNumber <= u.UnwindPoint {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
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

func unwindCallTraces(logPrefix string, db ethdb.RwTx, from, to uint64, quitCh <-chan struct{}, cfg CallTracesCfg) error {
	froms := map[string]struct{}{}
	tos := map[string]struct{}{}

	tracer := NewCallTracer()
	vmConfig := &vm.Config{Debug: true, NoReceipts: true, Tracer: tracer}
	for blockNum := from; blockNum > to; blockNum-- {
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
			return fmt.Errorf("no block %d", blockNum)
		}

		stateReader := state.NewPlainKvState(db, blockNum-1)
		stateWriter := state.NewNoopWriter()
		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(db, hash, number) }
		if _, err = core.ExecuteBlockEphemerally(cfg.chainConfig, vmConfig, getHeader, cfg.engine, block, stateReader, stateWriter); err != nil {
			return fmt.Errorf("exec block: %w", err)
		}

		coinbase := block.Coinbase()
		tos[string(coinbase[:])] = struct{}{}
		for _, uncle := range block.Uncles() {
			tos[string(uncle.Coinbase[:])] = struct{}{}
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

	if err := truncateBitmaps(db, dbutils.CallFromIndex, froms, to); err != nil {
		return err
	}
	if err := truncateBitmaps(db, dbutils.CallToIndex, tos, to); err != nil {
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
	ct.froms[from] = struct{}{}
	ct.tos[to] = struct{}{}
	return nil
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct *CallTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	ct.froms[from] = struct{}{}
	ct.tos[to] = struct{}{}
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
