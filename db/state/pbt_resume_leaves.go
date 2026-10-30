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
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"golang.org/x/sync/errgroup"
)

type PBinResumeOptions struct {
	BaseTxNum       uint64
	BaseState       []byte
	BaseStateEnd    uint64
	SourceRanges    [][2]uint64
	SourceTxNum     uint64
	Workers         int
	PostCancun      bool
	BytecodeRewrite bool
	Progress        PBinResumeProgressFunc
	GapKeys         *atomic.Uint64
}

type PBinResumeProgressFunc func(uint64)

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
		collectors[worker] = etl.NewCollector(fmt.Sprintf("pbin-resume-leaves-%d", worker), at.Dirs().Tmp, etl.NewSortableBuffer(pbinLeafWorkerBuffer(workers)), log.Root()).MergeInBackground(true)
	}
	defer func() {
		for _, collector := range collectors {
			collector.Close()
		}
	}()

	pin := at.Pin()
	defer pin.Close()
	ranges := pbinLeafAddressRanges(workers)
	g, walkCtx := errgroup.WithContext(ctx)
	for worker := range workers {
		g.Go(func() error {
			workerAt := pin.BeginFilesRo()
			defer workerAt.Close()
			for rangeIndex := worker; rangeIndex < len(ranges); rangeIndex += workers {
				if err := pbinResumeWalkRange(walkCtx, workerAt, ranges[rangeIndex], opts, collectors[worker]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var reported uint64
	var done uint64
	err := pbinLoadSortedLeafCollectors(collectors, func(leaf PBinLeaf) error {
		done++
		if opts.GapKeys != nil {
			opts.GapKeys.Store(done)
		}
		if done-reported >= 4096 {
			if opts.Progress != nil {
				opts.Progress(done - reported)
			}
			reported = done
		}
		if err := emit(leaf); err != nil {
			return err
		}
		return nil
	}, nil)
	if done != reported && opts.Progress != nil {
		opts.Progress(done - reported)
	}
	return err
}

func pbinResumeValidateSource(at *AggregatorRoTx, opts PBinResumeOptions) error {
	var hasGap bool
	baseEnd := pbinResumeBaseEnd(opts.BaseTxNum)
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
			if file.startTxNum == 0 && file.endTxNum > baseEnd {
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

func pbinResumeBaseEnd(baseTxNum uint64) uint64 {
	if baseTxNum < ^uint64(0) {
		return baseTxNum + 1
	}
	return baseTxNum
}

func pbinResumeWalkRange(ctx context.Context, at *AggregatorRoTx, addressRange pbinLeafAddressRange, opts PBinResumeOptions, collector *etl.Collector) error {
	domains := [...]kv.Domain{kv.AccountsDomain, kv.CodeDomain, kv.StorageDomain}
	var cursors [len(domains)]pbinLatestCursor
	for i, domain := range domains {
		iter, err := at.d[domain].debugRangeLatestFromFilesAfter(addressRange.from, addressRange.to, kv.Unlim, pbinResumeBaseEnd(opts.BaseTxNum))
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
		var storageReadSlots [][]byte
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
			storageReadSlots = append(storageReadSlots, bytes.Clone(slot))
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
		reader.storageAliases = make(map[string][]byte, len(storageReadSlots))
		for _, slot := range storageReadSlots {
			padded := make([]byte, eip8297.ValueLength)
			copy(padded, slot)
			normalizedKey := append(append([]byte(nil), address...), padded...)
			rawKey := append(append([]byte(nil), address...), slot...)
			reader.storageAliases[string(normalizedKey)] = rawKey
		}
		if err := pbinResumeEmitAddress(address, accountStamp, codeStamp, storageStamp, codeTouched, storageReadSlots, storageSlots, storageStamps, reader, collector); err != nil {
			return err
		}
	}
	return nil
}

func pbinResumeEmitAddress(address []byte, accountStamp, codeStamp, storageStamp uint64, codeTouched bool, storageReadSlots, storageSlots [][]byte, storageStamps map[string]uint64, reader *pbinResumeStateReader, collector *etl.Collector) error {
	account, err := commitmentdb.BinFeedAccountFromState(address, storageReadSlots, true, false, reader)
	if err != nil {
		return err
	}
	headerStamp := max(accountStamp, codeStamp, storageStamp)
	if !account.Exists {
		for _, key := range [][]byte{
			eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey),
			eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey),
			eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey),
		} {
			if collectErr := pbinResumeCollectOp(collector, pbt.Op{Key: key}, headerStamp); collectErr != nil {
				return collectErr
			}
		}
		for _, slot := range storageSlots {
			if collectErr := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyStorage(address, slot)}, storageStamps[string(eip8297.TreeKeyStorage(address, slot))]); collectErr != nil {
				return collectErr
			}
		}
		return nil
	}
	codeSize := uint64(len(account.Code))
	basic, err := eip8297.EncodeBasicData(account.Nonce, &account.Balance, codeSize)
	if err != nil {
		return err
	}
	if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: basic}, headerStamp); err != nil {
		return err
	}
	codeHash := eip8297.CodeHashValue(account.CodeHash)
	delegation := [eip8297.ValueLength]byte{}
	if eip8297.IsDelegation(account.Code) {
		codeHash = [eip8297.ValueLength]byte{}
		delegation = eip8297.EncodeDelegation(account.Code)
	}
	if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), Value: codeHash}, headerStamp); err != nil {
		return err
	}
	if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey), Value: delegation}, headerStamp); err != nil {
		return err
	}
	if codeTouched && !eip8297.IsDelegation(account.Code) {
		for index, chunk := range eip8297.ChunkifyCode(account.Code) {
			if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyCodeChunk(account.CodeHash, index), Value: chunk}, codeStamp); err != nil {
				return err
			}
		}
	}
	storageValues := make(map[string][]byte, len(account.Slots))
	for _, slot := range account.Slots {
		normalized := eip8297.RightAlign32(slot.Key)
		storageValues[string(normalized[:])] = slot.Value
	}
	for _, slotKey := range storageSlots {
		stamp := storageStamps[string(eip8297.TreeKeyStorage(address, slotKey))]
		if err := pbinResumeCollectOp(collector, pbt.Op{Key: eip8297.TreeKeyStorage(address, slotKey), Value: eip8297.EncodeStorageValue(storageValues[string(slotKey)])}, stamp); err != nil {
			return err
		}
	}
	return nil
}

func pbinResumeCollectOp(collector *etl.Collector, op pbt.Op, stamp uint64) error {
	var value [8 + eip8297.ValueLength]byte
	binary.BigEndian.PutUint64(value[:8], stamp)
	copy(value[8:], op.Value[:])
	return collector.Collect(op.Key, value[:])
}

type pbinResumeStateReader struct {
	at             *AggregatorRoTx
	sourceTxNum    uint64
	stepSize       uint64
	storageAliases map[string][]byte
}

func (r *pbinResumeStateReader) WithHistory() bool { return false }

func (r *pbinResumeStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *pbinResumeStateReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	readKey := key
	if domain == kv.StorageDomain {
		if alias, ok := r.storageAliases[string(key)]; ok {
			readKey = alias
		}
	}
	value, found, _, endTxNum, err := r.at.DebugGetLatestFromFiles(domain, readKey, r.sourceTxNum)
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
