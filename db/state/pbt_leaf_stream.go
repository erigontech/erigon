// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type PBinLeaf struct {
	Key   []byte
	Value []byte
	Stamp uint64
}

type pbinLatestCursor struct {
	iter  *DomainLatestIterFile
	key   []byte
	value []byte
	stamp uint64
	ok    bool
}

func ForEachPBinLeaf(at *AggregatorRoTx, roTx kv.Tx, filesOnly bool, emit func(PBinLeaf) error) error {
	return ForEachPBinLeafWithWorkers(at, roTx, filesOnly, 1, nil, emit)
}

func ForEachPBinLeafWithWorkers(at *AggregatorRoTx, roTx kv.Tx, filesOnly bool, workers int, workerRuns *atomic.Int32, emit func(PBinLeaf) error) error {
	if at == nil {
		return fmt.Errorf("pbin leaf stream: nil aggregator transaction")
	}
	if emit == nil {
		return fmt.Errorf("pbin leaf stream: nil emitter")
	}
	if !filesOnly && roTx == nil {
		return fmt.Errorf("pbin leaf stream: nil database transaction")
	}
	workers = max(workers, 1)
	if workers > 1 && filesOnly {
		return pbinForEachLeafParallel(at, roTx, filesOnly, workers, workerRuns, emit)
	}
	return pbinForEachLeafSerial(at, roTx, filesOnly, emit)
}

func VerifyPBinSourceStamps(ctx context.Context, at *AggregatorRoTx, roTx kv.Tx, filesOnly bool, endTxNum uint64, workers int, workerRuns *atomic.Int32, report func(uint64)) error {
	if at == nil {
		return fmt.Errorf("pbin stamp walk: nil aggregator transaction")
	}
	if !filesOnly && roTx == nil {
		return fmt.Errorf("pbin stamp walk: nil database transaction")
	}
	if !filesOnly {
		workers = 1
	}
	workers = max(workers, 1)
	ranges := pbinStampAddressRanges(workers)
	pin := at.Pin()
	defer pin.Close()
	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var errorMu sync.Mutex
	var firstErr error
	recordError := func(err error) {
		errorMu.Lock()
		if firstErr == nil || errors.Is(firstErr, context.Canceled) && !errors.Is(err, context.Canceled) {
			firstErr = err
		}
		errorMu.Unlock()
		cancel()
	}
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			if workerRuns != nil {
				workerRuns.Add(1)
			}
			for rangeIndex := worker; rangeIndex < len(ranges); rangeIndex += workers {
				if err := verifyPBinSourceStampRange(walkCtx, pin, roTx, filesOnly, ranges[rangeIndex], endTxNum, report); err != nil {
					recordError(err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	errorMu.Lock()
	defer errorMu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func verifyPBinSourceStampRange(ctx context.Context, pin *AggregatorFilesPin, roTx kv.Tx, filesOnly bool, addressRange pbinLeafAddressRange, endTxNum uint64, report func(uint64)) error {
	domains := [...]kv.Domain{kv.AccountsDomain, kv.CodeDomain, kv.StorageDomain}
	var cursors [len(domains)]pbinLatestCursor
	workerAt := pin.BeginFilesRo()
	defer workerAt.Close()
	for i, domain := range domains {
		var err error
		cursors[i], err = pbinOpenLatestCursor(workerAt, roTx, domain, filesOnly, addressRange.from, addressRange.to)
		if err != nil {
			for j := i - 1; j >= 0; j-- {
				cursors[j].close()
			}
			return err
		}
	}
	defer func() {
		for i := range cursors {
			cursors[i].close()
		}
	}()
	for i := range cursors {
		if err := cursors[i].advance(); err != nil {
			return err
		}
	}
	var done, reported uint64
	defer func() {
		if report != nil {
			report(done - reported)
		}
	}()
	for {
		cursor := -1
		for i := range cursors {
			if !cursors[i].ok || cursor >= 0 && bytes.Compare(cursors[i].key, cursors[cursor].key) >= 0 {
				continue
			}
			cursor = i
		}
		if cursor < 0 {
			return nil
		}
		current := &cursors[cursor]
		if current.stamp > endTxNum {
			return fmt.Errorf("pbin stamp walk: key %x has stamp %d after conversion txNum %d", current.key, current.stamp, endTxNum)
		}
		done++
		if report != nil && done-reported >= 4096 {
			report(done - reported)
			reported = done
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := current.advance(); err != nil {
			return err
		}
	}
}

func pbinForEachLeafSerial(at *AggregatorRoTx, roTx kv.Tx, filesOnly bool, emit func(PBinLeaf) error) error {
	domains := [...]kv.Domain{kv.AccountsDomain, kv.CodeDomain, kv.StorageDomain}
	var cursors [len(domains)]pbinLatestCursor
	for i, domain := range domains {
		var err error
		cursors[i], err = pbinOpenLatestCursor(at, roTx, domain, filesOnly, nil, nil)
		if err != nil {
			for j := i - 1; j >= 0; j-- {
				cursors[j].close()
			}
			return err
		}
	}
	defer func() {
		for i := len(cursors) - 1; i >= 0; i-- {
			cursors[i].close()
		}
	}()
	for i := range cursors {
		if err := cursors[i].advance(); err != nil {
			return err
		}
	}
	collector := etl.NewCollector("pbin-leaf-stream", at.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true).MergeInBackground(true)
	defer collector.Close()
	progress := NewPBTProgress(PBTProgressConfig{Message: "PBT leaf stream progress", Phase: "source leaf walk", TempDir: at.Dirs().Tmp})
	err := pbinWalkLeafRange(cursors[0], cursors[1], cursors[2], collector, progress.Add)
	progress.Done()
	if err != nil {
		return err
	}
	leafProgress := NewPBTProgress(PBTProgressConfig{Message: "PBT leaf stream progress", Phase: "leaf load", Total: collector.Collected(), TempDir: at.Dirs().Tmp})
	return pbinLoadSortedLeaves(collector, emit, leafProgress)
}

type pbinLeafAddressRange struct {
	from []byte
	to   []byte
}

func pbinForEachLeafParallel(at *AggregatorRoTx, roTx kv.Tx, filesOnly bool, workers int, workerRuns *atomic.Int32, emit func(PBinLeaf) error) error {
	ranges := pbinLeafAddressRanges(workers)
	const totalBufferBytes = 2 * datasize.GB
	perWorkerBuffer := (totalBufferBytes / datasize.ByteSize(workers)) * 7 / 8
	collectors := make([]*etl.Collector, workers)
	for i := range collectors {
		collectors[i] = etl.NewCollector(fmt.Sprintf("pbin-leaf-stream-%d", i), at.Dirs().Tmp, etl.NewSortableBuffer(perWorkerBuffer), log.Root()).MergeInBackground(true)
	}
	defer func() {
		for _, collector := range collectors {
			collector.Close()
		}
	}()
	pin := at.Pin()
	defer pin.Close()
	progress := NewPBTProgress(PBTProgressConfig{
		Message: "PBT leaf stream progress",
		Phase:   "source leaf walk",
		Fields:  []any{"workers", workers, "etl_a_buffer_budget_bytes", uint64(totalBufferBytes), "etl_a_buffer_bytes", uint64(0)},
		TempDir: at.Dirs().Tmp,
	})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	allocated := make([]atomic.Uint64, workers)
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerAt := pin.BeginFilesRo()
			defer workerAt.Close()
			if workerRuns != nil {
				workerRuns.Add(1)
			}
			collector := collectors[worker]
			report := func(n uint64) {
				allocated[worker].Store(uint64(collector.InMemoryCapacity()))
				var total uint64
				for i := range allocated {
					total += allocated[i].Load()
				}
				progress.SetFields("workers", workers, "etl_a_buffer_budget_bytes", uint64(totalBufferBytes), "etl_a_buffer_bytes", total)
				progress.Add(n)
			}
			for rangeIndex := worker; rangeIndex < len(ranges); rangeIndex += workers {
				r := ranges[rangeIndex]
				var cursors [3]pbinLatestCursor
				domains := [...]kv.Domain{kv.AccountsDomain, kv.CodeDomain, kv.StorageDomain}
				var err error
				for i, domain := range domains {
					cursors[i], err = pbinOpenLatestCursor(workerAt, roTx, domain, filesOnly, r.from, r.to)
					if err != nil {
						for j := i - 1; j >= 0; j-- {
							cursors[j].close()
						}
						break
					}
				}
				if err != nil {
					errs <- err
					return
				}
				for i := range cursors {
					if err = cursors[i].advance(); err != nil {
						break
					}
				}
				if err == nil {
					err = pbinWalkLeafRange(cursors[0], cursors[1], cursors[2], collector, report)
					report(0)
				}
				for i := len(cursors) - 1; i >= 0; i-- {
					cursors[i].close()
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	var totalAllocated uint64
	for i, collector := range collectors {
		allocated[i].Store(uint64(collector.InMemoryCapacity()))
		totalAllocated += allocated[i].Load()
	}
	progress.SetFields("workers", workers, "etl_a_buffer_budget_bytes", uint64(totalBufferBytes), "etl_a_buffer_bytes", totalAllocated)
	progress.Done()
	select {
	case err := <-errs:
		return err
	default:
	}
	total := uint64(0)
	for _, collector := range collectors {
		total += collector.Collected()
	}
	leafProgress := NewPBTProgress(PBTProgressConfig{Message: "PBT leaf stream progress", Phase: "leaf load", Total: total, TempDir: at.Dirs().Tmp})
	return pbinLoadSortedLeafCollectors(collectors, emit, leafProgress)
}

func pbinLeafAddressRanges(workers int) []pbinLeafAddressRange {
	return pbinAddressRanges(4 * workers)
}

func pbinStampAddressRanges(workers int) []pbinLeafAddressRange {
	return pbinAddressRanges(workers)
}

func pbinAddressRanges(rangeCount int) []pbinLeafAddressRange {
	space := new(big.Int).Lsh(big.NewInt(1), uint(pbinAddressLength*8))
	count := big.NewInt(int64(rangeCount))
	ranges := make([]pbinLeafAddressRange, rangeCount)
	for i := range ranges {
		from := new(big.Int).Mul(space, big.NewInt(int64(i)))
		from.Quo(from, count)
		to := new(big.Int).Mul(space, big.NewInt(int64(i+1)))
		to.Quo(to, count)
		ranges[i] = pbinLeafAddressRange{from: pbinAddressBoundary(from), to: pbinAddressBoundary(to)}
	}
	return ranges
}

func pbinAddressBoundary(value *big.Int) []byte {
	if value.Sign() == 0 || value.Bit(pbinAddressLength*8) != 0 {
		return nil
	}
	result := make([]byte, pbinAddressLength)
	valueBytes := value.Bytes()
	copy(result[len(result)-len(valueBytes):], valueBytes)
	return result
}

func pbinWalkLeafRange(accountsCursor, codeCursor, storageCursor pbinLatestCursor, collector *etl.Collector, report func(uint64)) error {
	emitter := pbt.NewRebuildFeedOpEmitter()
	var leafCollector [8 + eip8297.ValueLength]byte
	var sourceDone, sourceReported uint64
	defer func() { report(sourceDone - sourceReported) }()
	for accountsCursor.ok || codeCursor.ok || storageCursor.ok {
		address, err := pbinNextAddress(accountsCursor, codeCursor, storageCursor)
		if err != nil {
			return err
		}
		sourceDone++
		if sourceDone-sourceReported >= 4096 {
			report(sourceDone - sourceReported)
			sourceReported = sourceDone
		}
		if accountsCursor.ok && bytes.Equal(accountsCursor.key, address) {
			var account accounts.Account
			if err := accounts.DeserialiseV3(&account, accountsCursor.value); err != nil {
				return fmt.Errorf("pbin leaf stream: account %x: %w", address, err)
			}
			codeStamp := uint64(0)
			var code []byte
			if codeCursor.ok && bytes.Equal(codeCursor.key, address) {
				if !eip8297.IsEmptyCodeHash(account.CodeHash.Value()) {
					code = codeCursor.value
					codeStamp = codeCursor.stamp
				}
				if err := codeCursor.advance(); err != nil {
					return err
				}
			}
			leafStamp := max(accountsCursor.stamp, codeStamp)
			feedAccount := commitment.PBinFeedAccount{
				Address:     address,
				Exists:      true,
				Nonce:       account.Nonce,
				Balance:     account.Balance,
				CodeHash:    account.CodeHash.Value(),
				CodeWritten: true,
				Code:        code,
			}
			if err := emitter.EmitAccount(feedAccount, func(op pbt.Op) error {
				stamp := leafStamp
				if len(op.Key) > 0 && op.Key[0] == eip8297.CodeZone {
					stamp = codeStamp
				}
				return pbinCollectOp(collector, &leafCollector, op, stamp)
			}); err != nil {
				return err
			}
			if err := accountsCursor.advance(); err != nil {
				return err
			}
			for storageCursor.ok && bytes.Equal(storageCursor.key[:len(address)], address) {
				slot := commitment.PBinFeedSlot{Key: storageCursor.key[len(address):], Value: storageCursor.value}
				stamp := storageCursor.stamp
				if err := emitter.EmitStorageSlot(address, slot, func(op pbt.Op) error {
					return pbinCollectOp(collector, &leafCollector, op, stamp)
				}); err != nil {
					return err
				}
				if err := storageCursor.advance(); err != nil {
					return err
				}
			}
			continue
		}
		for storageCursor.ok && bytes.Equal(storageCursor.key[:len(address)], address) {
			if err := storageCursor.advance(); err != nil {
				return err
			}
		}
		if codeCursor.ok && bytes.Equal(codeCursor.key, address) {
			if err := codeCursor.advance(); err != nil {
				return err
			}
		}
	}
	return nil
}

func pbinOpenLatestCursor(at *AggregatorRoTx, roTx kv.Tx, domain kv.Domain, filesOnly bool, from, to []byte) (pbinLatestCursor, error) {
	domainRoTx := at.DbgDomain(domain)
	if domainRoTx == nil {
		return pbinLatestCursor{}, fmt.Errorf("pbin leaf stream: domain %s is unavailable", domain)
	}
	var iter *DomainLatestIterFile
	var err error
	if filesOnly {
		iter, err = domainRoTx.DebugRangeLatestFromFiles(from, to, kv.Unlim)
	} else {
		iter, err = domainRoTx.DebugRangeLatest(roTx, from, to, kv.Unlim)
	}
	if err != nil {
		return pbinLatestCursor{}, err
	}
	return pbinLatestCursor{iter: iter}, nil
}

func (c *pbinLatestCursor) close() {
	c.iter.Close()
}

func (c *pbinLatestCursor) advance() error {
	if !c.iter.HasNext() {
		c.ok = false
		c.key = nil
		c.value = nil
		return nil
	}
	key, value, err := c.iter.Next()
	if err != nil {
		return err
	}
	c.key = bytes.Clone(key)
	c.value = bytes.Clone(value)
	c.stamp = c.iter.Stamp()
	c.ok = true
	return nil
}

func pbinNextAddress(cursors ...pbinLatestCursor) ([]byte, error) {
	var address []byte
	for _, cursor := range cursors {
		if !cursor.ok {
			continue
		}
		if len(cursor.key) < pbinAddressLength {
			return nil, fmt.Errorf("pbin leaf stream: key has length %d, want at least %d", len(cursor.key), pbinAddressLength)
		}
		candidate := cursor.key[:pbinAddressLength]
		if address == nil || bytes.Compare(candidate, address) < 0 {
			address = candidate
		}
	}
	return address, nil
}

const pbinAddressLength = 20

func pbinCollectOp(collector *etl.Collector, scratch *[8 + eip8297.ValueLength]byte, op pbt.Op, stamp uint64) error {
	if op.Value == ([eip8297.ValueLength]byte{}) {
		return nil
	}
	binary.BigEndian.PutUint64(scratch[:], stamp)
	copy(scratch[8:], op.Value[:])
	return collector.Collect(op.Key, scratch[:])
}

type pbinStreamProgress struct {
	progress *PBTProgress
}

func (p *pbinStreamProgress) Add(n uint64) {
	if p.progress == nil {
		p.progress = NewPBTProgress(PBTProgressConfig{Message: "PBT leaf stream progress", Phase: "leaf load"})
	}
	p.progress.Add(n)
}

func (p *pbinStreamProgress) Done() {
	if p.progress != nil {
		p.progress.Done()
	}
}

func pbinLoadSortedLeaves(collector *etl.Collector, emit func(PBinLeaf) error, progress etl.Progress) error {
	return pbinLoadSortedLeafStream(func(load func([]byte, []byte) error) error {
		return collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			return load(key, value)
		}, etl.TransformArgs{Progress: progress})
	}, emit)
}

func pbinLoadSortedLeafCollectors(collectors []*etl.Collector, emit func(PBinLeaf) error, progress etl.Progress) error {
	return pbinLoadSortedLeafStream(func(load func([]byte, []byte) error) error {
		return etl.MergeLoad("pbin leaf stream", collectors, load, etl.TransformArgs{Progress: progress})
	}, emit)
}

func pbinLoadSortedLeafStream(load func(func([]byte, []byte) error) error, emit func(PBinLeaf) error) error {
	var previousKey, previousValue []byte
	var previousStamp uint64
	flush := func() error {
		if previousKey == nil {
			return nil
		}
		return emit(PBinLeaf{Key: previousKey, Value: previousValue, Stamp: previousStamp})
	}
	err := load(func(key, value []byte) error {
		if previousKey != nil && bytes.Equal(previousKey, key) {
			if !bytes.Equal(previousValue, value[8:]) {
				return fmt.Errorf("pbin leaf stream: conflicting values for key %x", key)
			}
			previousStamp = max(previousStamp, binary.BigEndian.Uint64(value))
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		previousKey = bytes.Clone(key)
		previousValue = bytes.Clone(value[8:])
		previousStamp = binary.BigEndian.Uint64(value)
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}
