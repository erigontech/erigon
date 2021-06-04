package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
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

	froms := map[string]*roaring64.Bitmap{}
	tos := map[string]*roaring64.Bitmap{}
	collectorFrom := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	collectorTo := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	checkFlushEvery := time.NewTicker(flushEvery)
	defer checkFlushEvery.Stop()

	traceCursor, err := tx.RwCursorDupSort(dbutils.CallTraceSet)
	if err != nil {
		return fmt.Errorf("%s: failed to create cursor for call traces: %w", logPrefix, err)
	}
	defer traceCursor.Close()

	var k, v []byte
	prev := startBlock
	for k, v, err = traceCursor.Seek(dbutils.EncodeBlockNumber(startBlock)); k != nil && err == nil; k, v, err = traceCursor.Next() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum > endBlock {
			break
		}
		if len(v) != common.AddressLength+1 {
			return fmt.Errorf("%s: wrong size of value in CallTraceSet: %x (size %d)", logPrefix, v, len(v))
		}
		mapKey := string(common.CopyBytes(v[:common.AddressLength]))
		if v[common.AddressLength]&1 > 0 {
			m, ok := froms[mapKey]
			if !ok {
				m = roaring64.New()
				froms[mapKey] = m
			}
			m.Add(blockNum)
		}
		if v[common.AddressLength]&2 > 0 {
			m, ok := tos[mapKey]
			if !ok {
				m = roaring64.New()
				tos[mapKey] = m
			}
			m.Add(blockNum)
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
			if needFlush64(froms, bufLimit) {
				if err := flushBitmaps64(collectorFrom, froms); err != nil {
					return fmt.Errorf("[%s] %w", logPrefix, err)
				}

				froms = map[string]*roaring64.Bitmap{}
			}

			if needFlush64(tos, bufLimit) {
				if err := flushBitmaps64(collectorTo, tos); err != nil {
					return fmt.Errorf("[%s] %w", logPrefix, err)
				}

				tos = map[string]*roaring64.Bitmap{}
			}
		}
	}
	if err != nil {
		return fmt.Errorf("%s: failed to move cursor: %w", logPrefix, err)
	}
	if err = flushBitmaps64(collectorFrom, froms); err != nil {
		return err
	}
	if err = flushBitmaps64(collectorTo, tos); err != nil {
		return err
	}
	// Clean up before loading call traces to reclaim space
	var prunedMin uint64 = math.MaxUint64
	var prunedMax uint64 = 0
	for k, _, err = traceCursor.First(); k != nil && err == nil; k, _, err = traceCursor.NextNoDup() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum+params.FullImmutabilityThreshold <= endBlock {
			break
		}
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Pruning call trace intermediate table", logPrefix), "number", blockNum,
				"alloc", common.StorageSize(m.Alloc),
				"sys", common.StorageSize(m.Sys),
				"numGC", int(m.NumGC))
		}
		if err = traceCursor.DeleteCurrentDuplicates(); err != nil {
			return fmt.Errorf("%s: failed to remove trace call set for block %d: %v", logPrefix, blockNum, err)
		}
		if blockNum < prunedMin {
			prunedMin = blockNum
		}
		if blockNum > prunedMax {
			prunedMax = blockNum
		}
	}
	if err != nil {
		return fmt.Errorf("%s: failed to move cleanup cursor: %w", logPrefix, err)
	}
	if prunedMax != 0 && prunedMax > prunedMin+16 {
		log.Info(fmt.Sprintf("[%s] Pruned call trace intermediate table", logPrefix), "from", prunedMin, "to", prunedMax)
	}
	if err := finaliseCallTraces(collectorFrom, collectorTo, logPrefix, tx, quit); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}
	return nil
}

func finaliseCallTraces(collectorFrom, collectorTo *etl.Collector, logPrefix string, tx ethdb.RwTx, quit <-chan struct{}) error {
	var currentBitmap = roaring64.New()
	var buf = bytes.NewBuffer(nil)
	lastChunkKey := make([]byte, 128)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if _, err := currentBitmap.ReadFrom(bytes.NewReader(v)); err != nil {
			return err
		}
		lastChunkKey = lastChunkKey[:len(k)+8]
		copy(lastChunkKey, k)
		binary.BigEndian.PutUint64(lastChunkKey[len(k):], ^uint64(0))
		lastChunkBytes, err := table.Get(lastChunkKey)
		if err != nil {
			return fmt.Errorf("find last chunk failed: %w", err)
		}

		if len(lastChunkBytes) > 0 {
			lastChunk := roaring64.New()
			_, err = lastChunk.ReadFrom(bytes.NewReader(lastChunkBytes))
			if err != nil {
				return fmt.Errorf("couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", err, len(lastChunkBytes))
			}
			currentBitmap.Or(lastChunk) // merge last existing chunk from db - next loop will overwrite it
		}
		if err := bitmapdb.WalkChunkWithKeys64(k, currentBitmap, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
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

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	traceCursor, err := db.RwCursorDupSort(dbutils.CallTraceSet)
	if err != nil {
		return fmt.Errorf("%s: failed to create cursor for call traces: %w", logPrefix, err)
	}

	var k, v []byte
	prev := to + 1
	for k, v, err = traceCursor.Seek(dbutils.EncodeBlockNumber(to + 1)); k != nil && err == nil; k, v, err = traceCursor.Next() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= from {
			break
		}
		if len(v) != common.AddressLength+1 {
			return fmt.Errorf("%s: wrong size of value in CallTraceSet: %x (size %d)", logPrefix, v, len(v))
		}
		mapKey := string(common.CopyBytes(v[:common.AddressLength]))
		if v[common.AddressLength]&1 > 0 {
			froms[mapKey] = struct{}{}
		}
		if v[common.AddressLength]&2 > 0 {
			tos[mapKey] = struct{}{}
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
		}
	}
	if err != nil {
		return fmt.Errorf("%s: failed to move cursor: %w", logPrefix, err)
	}

	if err := truncateBitmaps64(db, dbutils.CallFromIndex, froms, to); err != nil {
		return err
	}
	if err := truncateBitmaps64(db, dbutils.CallToIndex, tos, to); err != nil {
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
