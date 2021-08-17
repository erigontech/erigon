package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/log/v3"
)

type CallTracesCfg struct {
	db      kv.RwDB
	prune   prune.Mode
	ToBlock uint64 // not setting this params means no limit
	tmpdir  string
}

func StageCallTracesCfg(
	db kv.RwDB,
	prune prune.Mode,
	toBlock uint64,
	tmpdir string,
) CallTracesCfg {
	return CallTracesCfg{
		db:      db,
		prune:   prune,
		ToBlock: toBlock,
		tmpdir:  tmpdir,
	}
}

func SpawnCallTraces(s *StageState, tx kv.RwTx, cfg CallTracesCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	quit := ctx.Done()
	endBlock, err := s.ExecutionAt(tx)
	if cfg.ToBlock > 0 && cfg.ToBlock < endBlock {
		endBlock = cfg.ToBlock
	}
	logPrefix := s.LogPrefix()
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		return nil
	}

	if err := promoteCallTraces(logPrefix, tx, s.BlockNumber+1, endBlock, bitmapsBufLimit, bitmapsFlushEvery, quit, cfg.tmpdir); err != nil {
		return err
	}

	if err := s.Update(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func promoteCallTraces(logPrefix string, tx kv.RwTx, startBlock, endBlock uint64, bufLimit datasize.ByteSize, flushEvery time.Duration, quit <-chan struct{}, tmpdir string) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]*roaring64.Bitmap{}
	tos := map[string]*roaring64.Bitmap{}
	collectorFrom := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorFrom.Close(logPrefix)
	collectorTo := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorTo.Close(logPrefix)
	checkFlushEvery := time.NewTicker(flushEvery)
	defer checkFlushEvery.Stop()

	traceCursor, err := tx.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return fmt.Errorf("failed to create cursor: %w", err)
	}
	defer traceCursor.Close()

	var k, v []byte
	prev := startBlock
	for k, v, err = traceCursor.Seek(dbutils.EncodeBlockNumber(startBlock)); k != nil; k, v, err = traceCursor.Next() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum > endBlock {
			break
		}
		if len(v) != common.AddressLength+1 {
			return fmt.Errorf(" wrong size of value in CallTraceSet: %x (size %d)", v, len(v))
		}
		mapKey := string(v[:common.AddressLength])
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
				"sys", common.StorageSize(m.Sys))
		case <-checkFlushEvery.C:
			if needFlush64(froms, bufLimit) {
				if err := flushBitmaps64(collectorFrom, froms); err != nil {
					return err
				}

				froms = map[string]*roaring64.Bitmap{}
			}

			if needFlush64(tos, bufLimit) {
				if err := flushBitmaps64(collectorTo, tos); err != nil {
					return err
				}

				tos = map[string]*roaring64.Bitmap{}
			}
		}
	}
	if err = flushBitmaps64(collectorFrom, froms); err != nil {
		return err
	}
	if err = flushBitmaps64(collectorTo, tos); err != nil {
		return err
	}

	if err := finaliseCallTraces(collectorFrom, collectorTo, logPrefix, tx, quit); err != nil {
		return err
	}

	return nil
}

func finaliseCallTraces(collectorFrom, collectorTo *etl.Collector, logPrefix string, tx kv.RwTx, quit <-chan struct{}) error {
	var currentBitmap = roaring64.New()
	var buf = bytes.NewBuffer(nil)
	lastChunkKey := make([]byte, 128)
	reader := bytes.NewReader(nil)
	reader2 := bytes.NewReader(nil)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		reader.Reset(v)
		if _, err := currentBitmap.ReadFrom(reader); err != nil {
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
			reader2.Reset(lastChunkBytes)
			_, err = lastChunk.ReadFrom(reader2)
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
	if err := collectorFrom.Load(logPrefix, tx, kv.CallFromIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	if err := collectorTo.Load(logPrefix, tx, kv.CallToIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func UnwindCallTraces(u *UnwindState, s *StageState, tx kv.RwTx, cfg CallTracesCfg, ctx context.Context) (err error) {
	if s.BlockNumber <= u.UnwindPoint {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := u.LogPrefix()
	if s.BlockNumber-u.UnwindPoint > 16 {
		log.Info(fmt.Sprintf("[%s] Unwind", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)
	}
	if err := DoUnwindCallTraces(logPrefix, tx, s.BlockNumber, u.UnwindPoint, ctx, cfg.tmpdir); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func DoUnwindCallTraces(logPrefix string, db kv.RwTx, from, to uint64, ctx context.Context, tmpdir string) error {
	froms := etl.NewCollector(tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	tos := etl.NewCollector(tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	traceCursor, err := db.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return fmt.Errorf("create cursor for call traces: %w", err)
	}
	defer traceCursor.Close()

	var k, v []byte
	prev := to + 1
	for k, v, err = traceCursor.Seek(dbutils.EncodeBlockNumber(to + 1)); k != nil; k, v, err = traceCursor.Next() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= from {
			break
		}
		if len(v) != common.AddressLength+1 {
			return fmt.Errorf("wrong size of value in CallTraceSet: %x (size %d)", v, len(v))
		}
		mapKey := v[:common.AddressLength]
		if v[common.AddressLength]&1 > 0 {
			if err = froms.Collect(mapKey, nil); err != nil {
				return nil
			}
		}
		if v[common.AddressLength]&2 > 0 {
			if err = tos.Collect(mapKey, nil); err != nil {
				return nil
			}
		}
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			speed := float64(blockNum-prev) / float64(logInterval/time.Second)
			prev = blockNum

			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum,
				"blk/second", speed,
				"alloc", common.StorageSize(m.Alloc),
				"sys", common.StorageSize(m.Sys))
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
	}

	if err = froms.Load(logPrefix, db, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return bitmapdb.TruncateRange64(db, kv.CallFromIndex, k, to+1)
	}, etl.TransformArgs{}); err != nil {
		return fmt.Errorf("TruncateRange: bucket=%s, %w", kv.CallFromIndex, err)
	}

	if err = tos.Load(logPrefix, db, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return bitmapdb.TruncateRange64(db, kv.CallToIndex, k, to+1)
	}, etl.TransformArgs{}); err != nil {
		return fmt.Errorf("TruncateRange: bucket=%s, %w", kv.CallFromIndex, err)
	}
	return nil
}

type CallTracer struct {
	froms   map[common.Address]struct{}
	tos     map[common.Address]bool // address -> isCreated
	hasTEVM func(contractHash common.Hash) (bool, error)
}

func NewCallTracer(hasTEVM func(contractHash common.Hash) (bool, error)) *CallTracer {
	return &CallTracer{
		froms:   make(map[common.Address]struct{}),
		tos:     make(map[common.Address]bool),
		hasTEVM: hasTEVM,
	}
}

func (ct *CallTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) error {
	ct.froms[from] = struct{}{}

	created, ok := ct.tos[to]
	if !ok {
		ct.tos[to] = false
	}

	if !created && create {
		if len(code) > 0 && ct.hasTEVM != nil {
			has, err := ct.hasTEVM(common.BytesToHash(crypto.Keccak256(code)))
			if !has {
				ct.tos[to] = true
			}

			if err != nil {
				log.Warn("while CaptureStart", "error", err)
			}
		}
	}
	return nil
}
func (ct *CallTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CallTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) error {
	return nil
}
func (ct *CallTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	ct.froms[from] = struct{}{}
	ct.tos[to] = false
}
func (ct *CallTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ct *CallTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

func PruneCallTraces(s *PruneState, tx kv.RwTx, cfg CallTracesCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if cfg.prune.CallTraces.Enabled() {
		if err = pruneCallTraces(tx, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), ctx); err != nil {
			return err
		}
	}
	if err := s.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func pruneCallTraces(tx kv.RwTx, logPrefix string, pruneTo uint64, ctx context.Context) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]struct{}{}
	tos := map[string]struct{}{}

	{
		traceCursor, err := tx.CursorDupSort(kv.CallTraceSet)
		if err != nil {
			return fmt.Errorf("create cursor for call traces: %w", err)
		}
		defer traceCursor.Close()

		var k, v []byte
		for k, v, err = traceCursor.First(); k != nil; k, v, err = traceCursor.Next() {
			if err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum >= pruneTo {
				break
			}
			if len(v) != common.AddressLength+1 {
				return fmt.Errorf("wrong size of value in CallTraceSet: %x (size %d)", v, len(v))
			}
			mapKey := string(v[:common.AddressLength])
			if v[common.AddressLength]&1 > 0 {
				froms[mapKey] = struct{}{}
			}
			if v[common.AddressLength]&2 > 0 {
				tos[mapKey] = struct{}{}
			}
			select {
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
			case <-ctx.Done():
				return common.ErrStopped
			default:
			}
		}

	}

	{
		sorted := make([]string, 0, len(froms))
		for k := range froms {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		c, err := tx.RwCursor(kv.CallFromIndex)
		if err != nil {
			return err
		}
		defer c.Close()

		for _, fromS := range sorted {
			from := []byte(fromS)
			for k, _, err := c.Seek(from); k != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
				blockNum := binary.BigEndian.Uint64(k[common.AddressLength:])
				if !bytes.HasPrefix(k, from) || blockNum >= pruneTo {
					break
				}
				select {
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s]", logPrefix), "table", kv.CallFromIndex, "block", blockNum)
				case <-ctx.Done():
					return common.ErrStopped
				default:
				}
				if err = c.DeleteCurrent(); err != nil {
					return fmt.Errorf("failed delete, block=%d: %w", blockNum, err)
				}
			}
		}
	}
	{
		sorted := make([]string, 0, len(tos))
		for k := range tos {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		c, err := tx.RwCursor(kv.CallToIndex)
		if err != nil {
			return err
		}
		defer c.Close()

		for _, toS := range sorted {
			to := []byte(toS)
			for k, _, err := c.Seek(to); k != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
				blockNum := binary.BigEndian.Uint64(k[common.AddressLength:])
				if !bytes.HasPrefix(k, to) || blockNum >= pruneTo {
					break
				}
				select {
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s]", logPrefix), "table", kv.CallToIndex, "block", blockNum)
				case <-ctx.Done():
					return common.ErrStopped
				default:
				}
				if err = c.DeleteCurrent(); err != nil {
					return fmt.Errorf("failed delete, block=%d: %w", blockNum, err)
				}
			}
		}
	}
	return nil
}
