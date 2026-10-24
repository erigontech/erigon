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
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

type PBinResumeOptions struct {
	BaseTxNum       uint64
	SourceTxNum     uint64
	Workers         int
	PostCancun      bool
	BytecodeRewrite bool
}

func PBinResumeLeaves(ctx context.Context, at *AggregatorRoTx, opts PBinResumeOptions, emit func(PBinLeaf) error) error {
	if at == nil {
		return errors.New("pbin resume: nil aggregator transaction")
	}
	if emit == nil {
		return errors.New("pbin resume: nil emitter")
	}
	if opts.SourceTxNum <= opts.BaseTxNum {
		return fmt.Errorf("pbin resume: source txNum %d is not after base txNum %d", opts.SourceTxNum, opts.BaseTxNum)
	}
	workers := max(opts.Workers, 1)
	if err := pbinResumeValidateSource(at, opts); err != nil {
		return err
	}
	collectors := make([]*etl.Collector, workers)
	for worker := range workers {
		collectors[worker] = etl.NewCollector(fmt.Sprintf("pbin-resume-leaves-%d", worker), at.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true).MergeInBackground(true)
	}
	defer func() {
		for _, collector := range collectors {
			collector.Close()
		}
	}()

	pin := at.Pin()
	defer pin.Close()
	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ranges := pbinLeafAddressRanges(workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerAt := pin.BeginFilesRo()
			defer workerAt.Close()
			for rangeIndex := worker; rangeIndex < len(ranges); rangeIndex += workers {
				select {
				case <-walkCtx.Done():
					errs <- walkCtx.Err()
					return
				default:
				}
				if err := pbinResumeWalkRange(walkCtx, workerAt, ranges[rangeIndex], opts, collectors[worker]); err != nil {
					errs <- err
					cancel()
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return pbinLoadSortedLeafCollectors(collectors, emit, nil)
}

func pbinResumeValidateSource(at *AggregatorRoTx, opts PBinResumeOptions) error {
	var hasGap bool
	for _, domain := range [...]kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		domainRoTx := at.d[domain]
		if domainRoTx == nil {
			continue
		}
		for _, file := range domainRoTx.files {
			if file.endTxNum <= opts.BaseTxNum {
				continue
			}
			hasGap = true
			if file.startTxNum == 0 {
				return fmt.Errorf("pbin resume: source %s file starts at txNum 0 and spans base txNum %d", domain, opts.BaseTxNum)
			}
		}
	}
	if !hasGap {
		return nil
	}
	if !opts.PostCancun {
		return errors.New("pbin resume: gap is not entirely post-Cancun")
	}
	if opts.BytecodeRewrite {
		return errors.New("pbin resume: chain has a bytecode rewrite")
	}
	return nil
}

func pbinResumeWalkRange(ctx context.Context, at *AggregatorRoTx, addressRange pbinLeafAddressRange, opts PBinResumeOptions, collector *etl.Collector) error {
	domains := [...]kv.Domain{kv.AccountsDomain, kv.CodeDomain, kv.StorageDomain}
	var cursors [len(domains)]pbinLatestCursor
	for i, domain := range domains {
		iter, err := at.d[domain].debugRangeLatestFromFilesAfter(addressRange.from, addressRange.to, kv.Unlim, opts.BaseTxNum)
		if err != nil {
			for j := range i {
				cursors[j].close()
			}
			return err
		}
		cursors[i] = pbinLatestCursor{iter: iter}
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
	reader := &pbinResumeStateReader{at: at, sourceTxNum: opts.SourceTxNum, stepSize: at.StepSize()}
	emitter := pbt.NewRebuildFeedOpEmitter()
	for cursors[0].ok || cursors[1].ok || cursors[2].ok {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		address, err := pbinNextAddress(cursors[0], cursors[1], cursors[2])
		if err != nil {
			return err
		}
		var accountStamp, codeStamp, storageStamp uint64
		var codeTouched bool
		var accountTouched bool
		var storageSlots [][]byte
		storageStamps := make(map[string]uint64)
		if cursors[0].ok && bytes.Equal(cursors[0].key, address) {
			accountTouched = true
			accountStamp = cursors[0].stamp
			if err := cursors[0].advance(); err != nil {
				return err
			}
		}
		if cursors[1].ok && bytes.Equal(cursors[1].key, address) {
			codeTouched = true
			codeStamp = cursors[1].stamp
			if err := cursors[1].advance(); err != nil {
				return err
			}
		}
		for cursors[2].ok && bytes.HasPrefix(cursors[2].key, address) {
			slot := cursors[2].key[len(address):]
			if len(slot) > eip8297.ValueLength {
				return fmt.Errorf("pbin resume: storage slot for address %x has length %d", address, len(slot))
			}
			slot32 := eip8297.RightAlign32(slot)
			slotKey := bytes.Clone(slot32[:])
			storageSlots = append(storageSlots, slotKey)
			storageStamp = max(storageStamp, cursors[2].stamp)
			storageStamps[string(eip8297.TreeKeyStorage(address, slotKey))] = cursors[2].stamp
			if err := cursors[2].advance(); err != nil {
				return err
			}
		}
		if !accountTouched && !codeTouched && len(storageSlots) == 0 {
			continue
		}
		if err := pbinResumeEmitAddress(address, accountStamp, codeStamp, storageStamp, codeTouched, storageSlots, storageStamps, reader, emitter, collector); err != nil {
			return err
		}
	}
	return nil
}

func pbinResumeEmitAddress(address []byte, accountStamp, codeStamp, storageStamp uint64, codeTouched bool, storageSlots [][]byte, storageStamps map[string]uint64, reader commitmentdb.StateReader, emitter *pbt.FeedOpEmitter, collector *etl.Collector) error {
	account, err := commitmentdb.BinFeedAccountFromState(address, storageSlots, true, false, reader)
	if err != nil {
		return err
	}
	headerStamp := max(accountStamp, codeStamp, storageStamp)
	if !account.Exists {
		for index := range 3 {
			if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyAccount(address, byte(index))}, headerStamp); err != nil {
				return err
			}
		}
		for _, slot := range storageSlots {
			if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyStorage(address, slot)}, storageStamps[string(eip8297.TreeKeyStorage(address, slot))]); err != nil {
				return err
			}
		}
		return nil
	}
	return emitter.EmitAccount(account, func(op pbt.Op) error {
		if len(op.Key) == 0 {
			return errors.New("pbin resume: feed emitted a non-leaf operation")
		}
		if op.Key[0] == eip8297.CodeZone && !codeTouched {
			return nil
		}
		stamp := headerStamp
		switch op.Key[0] {
		case eip8297.CodeZone:
			stamp = codeStamp
		case eip8297.StorageZone:
			stamp = storageStamps[string(op.Key)]
		case eip8297.AccountZone:
			if len(op.Key) != eip8297.AccountKeyLength || op.Key[eip8297.AccountKeyLength-1] < eip8297.HeaderStorageOffset {
				break
			}
			stamp = storageStamps[string(op.Key)]
		}
		return pbinResumeCollectOp(collector, op, stamp)
	})
}

func pbinResumeCollectOp(collector *etl.Collector, op pbt.Op, stamp uint64) error {
	var value [8 + eip8297.ValueLength]byte
	binary.BigEndian.PutUint64(value[:8], stamp)
	copy(value[8:], op.Value[:])
	return collector.Collect(op.Key, value[:])
}

type pbinResumeStateReader struct {
	at          *AggregatorRoTx
	sourceTxNum uint64
	stepSize    uint64
}

func (r *pbinResumeStateReader) WithHistory() bool { return false }

func (r *pbinResumeStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *pbinResumeStateReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	value, found, _, endTxNum, err := r.at.DebugGetLatestFromFiles(domain, key, r.sourceTxNum)
	if err != nil {
		return nil, 0, err
	}
	if !found {
		return nil, 0, nil
	}
	return bytes.Clone(value), kv.Step(endTxNum / r.stepSize), nil
}

func (r *pbinResumeStateReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r *pbinResumeStateReader) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func (r *pbinResumeStateReader) BindsWorkerTx() bool { return false }
