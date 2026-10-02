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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

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
	iter        pbinLeafIterator
	key         []byte
	value       []byte
	stamp       uint64
	stampReader func() uint64
	ok          bool
}

type pbinLeafIterator interface {
	HasNext() bool
	Next() ([]byte, []byte, error)
	Close()
}

func ForEachPBinLeaf(at *AggregatorRoTx, roTx kv.Tx, filesOnly bool, emit func(PBinLeaf) error) error {
	if at == nil {
		return fmt.Errorf("pbin leaf stream: nil aggregator transaction")
	}
	if emit == nil {
		return fmt.Errorf("pbin leaf stream: nil emitter")
	}
	if !filesOnly && roTx == nil {
		return fmt.Errorf("pbin leaf stream: nil database transaction")
	}
	accountsCursor, err := pbinOpenLatestCursor(at, roTx, kv.AccountsDomain, filesOnly)
	if err != nil {
		return err
	}
	defer accountsCursor.close()
	codeCursor, err := pbinOpenLatestCursor(at, roTx, kv.CodeDomain, filesOnly)
	if err != nil {
		return err
	}
	defer codeCursor.close()
	storageCursor, err := pbinOpenLatestCursor(at, roTx, kv.StorageDomain, filesOnly)
	if err != nil {
		return err
	}
	defer storageCursor.close()
	if err := accountsCursor.advance(); err != nil {
		return err
	}
	if err := codeCursor.advance(); err != nil {
		return err
	}
	if err := storageCursor.advance(); err != nil {
		return err
	}
	return pbinForEachLeaf(at, accountsCursor, codeCursor, storageCursor, emit)
}

func pbinForEachLeaf(at *AggregatorRoTx, accountsCursor, codeCursor, storageCursor pbinLatestCursor, emit func(PBinLeaf) error) error {
	collector := etl.NewCollector("pbin-leaf-stream", at.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true)
	defer collector.Close()
	emitter := pbt.NewRebuildFeedOpEmitter()
	leafCollector := pbinLeafCollector{}
	progress := pbinStreamProgress{next: time.Now().Add(30 * time.Second)}
	for accountsCursor.ok || codeCursor.ok || storageCursor.ok {
		address, err := pbinNextAddress(accountsCursor, codeCursor, storageCursor)
		if err != nil {
			return err
		}
		progress.account(address)
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
			accountStamp := accountsCursor.stamp
			leafStamp := max(accountStamp, codeStamp)
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
		if storageCursor.ok && bytes.Equal(storageCursor.key[:len(address)], address) {
			if err := pbinSkipAddress(&storageCursor, address); err != nil {
				return err
			}
		}
		if codeCursor.ok && bytes.Equal(codeCursor.key, address) {
			if err := codeCursor.advance(); err != nil {
				return err
			}
		}
	}
	return pbinLoadSortedLeaves(collector, emit, &progress)
}

type pbinStreamProgress struct {
	next     time.Time
	accounts uint64
	leaves   uint64
}

func (p *pbinStreamProgress) account(key []byte) {
	p.accounts++
	if p.accounts&4095 != 0 {
		return
	}
	p.report("leaf collection", key)
}

func (p *pbinStreamProgress) leaf(key []byte) {
	p.leaves++
	if p.leaves&4095 != 0 {
		return
	}
	p.report("leaf load", key)
}

func (p *pbinStreamProgress) report(phase string, key []byte) {
	now := time.Now()
	if now.Before(p.next) {
		return
	}
	p.next = now.Add(30 * time.Second)
	prefix := key
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	log.Root().Info("PBT leaf stream progress", "phase", phase, "accounts", p.accounts, "leaves", p.leaves, "key_prefix", hex.EncodeToString(prefix))
}

func pbinOpenLatestCursor(at *AggregatorRoTx, roTx kv.Tx, domain kv.Domain, filesOnly bool) (pbinLatestCursor, error) {
	domainRoTx := at.DbgDomain(domain)
	if domainRoTx == nil {
		return pbinLatestCursor{}, fmt.Errorf("pbin leaf stream: domain %s is unavailable", domain)
	}
	var iter pbinLeafIterator
	var stampReader func() uint64
	var err error
	if filesOnly {
		fileIter, fileErr := domainRoTx.DebugRangeLatestFromFiles(nil, nil, kv.Unlim)
		iter, err = fileIter, fileErr
		if fileIter != nil {
			stampReader = fileIter.Stamp
		}
	} else {
		dbIter, dbErr := domainRoTx.DebugRangeLatest(roTx, nil, nil, kv.Unlim)
		iter, err = dbIter, dbErr
		if dbIter != nil {
			stampReader = dbIter.Stamp
		}
	}
	if err != nil {
		return pbinLatestCursor{}, err
	}
	return pbinLatestCursor{iter: iter, stampReader: stampReader}, nil
}

func (c *pbinLatestCursor) close() {
	if c.iter != nil {
		c.iter.Close()
	}
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
	if c.stampReader != nil {
		c.stamp = c.stampReader()
	} else {
		c.stamp = 0
	}
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

func pbinSkipAddress(cursor *pbinLatestCursor, address []byte) error {
	for cursor.ok && bytes.Equal(cursor.key[:len(address)], address) {
		if err := cursor.advance(); err != nil {
			return err
		}
	}
	return nil
}

func pbinCollectOp(collector *etl.Collector, scratch *pbinLeafCollector, op pbt.Op, stamp uint64) error {
	if len(op.Value) == 0 || pbinAllZero(op.Value[:]) {
		return nil
	}
	return scratch.collect(collector, op.Key, op.Value[:], stamp)
}

func pbinAllZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

func pbinCollectLeaf(collector *etl.Collector, leaf PBinLeaf) error {
	return (&pbinLeafCollector{}).collect(collector, leaf.Key, leaf.Value, leaf.Stamp)
}

type pbinLeafCollector struct {
	value [8 + eip8297.ValueLength]byte
}

func (c *pbinLeafCollector) collect(collector *etl.Collector, key, value []byte, stamp uint64) error {
	if collector == nil {
		return fmt.Errorf("pbin leaf stream: nil collector")
	}
	if len(value) != eip8297.ValueLength {
		return fmt.Errorf("pbin leaf stream: value has length %d, want %d", len(value), eip8297.ValueLength)
	}
	binary.BigEndian.PutUint64(c.value[:], stamp)
	copy(c.value[8:], value)
	return collector.Collect(key, c.value[:])
}

func pbinLoadSortedLeaves(collector *etl.Collector, emit func(PBinLeaf) error, progress *pbinStreamProgress) error {
	if collector == nil {
		return fmt.Errorf("pbin leaf stream: nil collector")
	}
	if emit == nil {
		return fmt.Errorf("pbin leaf stream: nil emitter")
	}
	var previousKey, previousValue []byte
	var previousStamp uint64
	flush := func() error {
		if previousKey == nil {
			return nil
		}
		return emit(PBinLeaf{Key: previousKey, Value: previousValue, Stamp: previousStamp})
	}
	err := collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if len(value) != 8+eip8297.ValueLength {
			return fmt.Errorf("pbin leaf stream: encoded value has length %d, want %d", len(value), 8+eip8297.ValueLength)
		}
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
		if progress != nil {
			progress.leaf(key)
		}
		previousKey = bytes.Clone(key)
		previousValue = bytes.Clone(value[8:])
		previousStamp = binary.BigEndian.Uint64(value)
		return nil
	}, etl.TransformArgs{})
	if err != nil {
		return err
	}
	return flush()
}
