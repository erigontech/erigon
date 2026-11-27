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
	"runtime"
	"sync"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
)

// Two chunks run in about half the serial time from 256 write sets.
const notifyWriteSetsPerWorker = 128

type notifyOpKind uint8

const (
	notifyCode notifyOpKind = iota
	notifyStorage
	notifyAccount
)

type notifyOp struct {
	kind  notifyOpKind
	at    int32
	key   accounts.StorageKey
	value []byte
}

type notifiedAccount struct {
	addr        accounts.Address
	incarnation uint64
	changed     bool
	resolved    bool
	data        []byte
}

// notifyChunk holds the accumulator calls of consecutive write sets, in call order.
type notifyChunk struct {
	ops     []notifyOp
	touched []notifiedAccount
	err     error
}

// NotifyAccumulator sends a block's state changes to the accumulator. It reads every
// account's latest state, so the block's writes must already be applied. StartChange
// must be called on the accumulator first.
func (rs *StateV3) NotifyAccumulator(accumulator *shards.Accumulator, roTx kv.TemporalTx, blockWrites []WriteSetView) error {
	if accumulator == nil {
		return nil
	}
	workers := min(runtime.GOMAXPROCS(0), len(blockWrites)/notifyWriteSetsPerWorker)
	if workers < 2 {
		return rs.notifyAccumulatorSerial(accumulator, roTx, blockWrites)
	}
	chunks := make([]notifyChunk, workers)
	n := len(blockWrites)
	var wg sync.WaitGroup
	for k := range chunks {
		wg.Go(func() { chunks[k].collect(rs.domains, blockWrites[k*n/workers:(k+1)*n/workers]) })
	}
	wg.Wait()
	entries := 0
	for k := range chunks {
		if err := chunks[k].resolveFromTx(rs.domains, roTx); err != nil {
			return err
		}
		entries += len(chunks[k].touched)
	}
	if entries > 0 {
		accumulator.Reserve(entries)
	}
	// A chunk repeats ChangeAccount for an account an earlier chunk changed; the
	// repeat carries the same final state, so the entry stays as it was.
	for k := range chunks {
		chunks[k].replay(accumulator)
	}
	return nil
}

func (rs *StateV3) notifyAccumulatorSerial(accumulator *shards.Accumulator, roTx kv.TemporalTx, blockWrites []WriteSetView) error {
	latest := make(map[accounts.Address]accounts.Account, len(blockWrites))
	account := func(addr accounts.Address) (accounts.Account, error) {
		if acc, ok := latest[addr]; ok {
			return acc, nil
		}
		acc := accounts.NewAccount()
		address := addr.Value()
		v, _, err := rs.domains.GetLatest(kv.AccountsDomain, roTx, address[:])
		if err != nil {
			return accounts.Account{}, err
		}
		if len(v) > 0 {
			if err := accounts.DeserialiseV3(&acc, v); err != nil {
				return accounts.Account{}, err
			}
		}
		latest[addr] = acc
		return acc, nil
	}

	// Every call for an address carries its final state, so a repeat would change nothing.
	changed := make(map[accounts.Address]struct{}, len(blockWrites))
	changeAccount := func(addr accounts.Address) error {
		if _, ok := changed[addr]; ok {
			return nil
		}
		changed[addr] = struct{}{}
		acc, err := account(addr)
		if err != nil {
			return err
		}
		accumulator.ChangeAccount(addr.Value(), acc.Incarnation, accounts.SerialiseV3(&acc))
		return nil
	}

	for _, writes := range blockWrites {
		// Balance and nonce writes only name the account: range the keys, not the resolved values.
		keys := writes
		if v, ok := writes.(*versionMapWriteView); ok {
			keys = v.keys
		}
		for addr, write := range writes.Codes() {
			acc, err := account(addr)
			if err != nil {
				return err
			}
			accumulator.ChangeCode(addr.Value(), acc.Incarnation, write.Val.Bytes)
		}
		for addr, byKey := range writes.Storages() {
			acc, err := account(addr)
			if err != nil {
				return err
			}
			for key, write := range byKey {
				accumulator.ChangeStorage(addr.Value(), acc.Incarnation, key.Value(), write.Val.Bytes())
			}
		}
		for addr := range keys.Balances() {
			if err := changeAccount(addr); err != nil {
				return err
			}
		}
		for addr := range keys.Nonces() {
			if err := changeAccount(addr); err != nil {
				return err
			}
		}
	}
	return nil
}

// collect records the accumulator calls of blockWrites and resolves their accounts
// from the mem batch. A miss is left for resolveFromTx: roTx must stay on one goroutine.
func (c *notifyChunk) collect(domains *execctx.SharedDomains, blockWrites []WriteSetView) {
	c.ops = make([]notifyOp, 0, 2*len(blockWrites))
	c.touched = make([]notifiedAccount, 0, len(blockWrites))
	index := make(map[accounts.Address]int32, len(blockWrites))
	at := func(addr accounts.Address) int32 {
		i, ok := index[addr]
		if !ok {
			i = int32(len(c.touched))
			index[addr] = i
			c.touched = append(c.touched, notifiedAccount{addr: addr})
		}
		return i
	}
	// Every call for an address carries its final state, so a repeat would change nothing.
	changeAccount := func(addr accounts.Address) {
		i := at(addr)
		if !c.touched[i].changed {
			c.touched[i].changed = true
			c.ops = append(c.ops, notifyOp{kind: notifyAccount, at: i})
		}
	}
	for _, writes := range blockWrites {
		// Balance and nonce writes only name the account: range the keys, not the resolved values.
		keys := writes
		if v, ok := writes.(*versionMapWriteView); ok {
			keys = v.keys
		}
		for addr, write := range writes.Codes() {
			c.ops = append(c.ops, notifyOp{kind: notifyCode, at: at(addr), value: write.Val.Bytes})
		}
		for addr, byKey := range writes.Storages() {
			i := at(addr)
			for key, write := range byKey {
				c.ops = append(c.ops, notifyOp{kind: notifyStorage, at: i, key: key, value: write.Val.Bytes()})
			}
		}
		for addr := range keys.Balances() {
			changeAccount(addr)
		}
		for addr := range keys.Nonces() {
			changeAccount(addr)
		}
	}
	for i := range c.touched {
		address := c.touched[i].addr.Value()
		if v, _, ok := domains.GetLatestFromMemory(kv.AccountsDomain, address[:]); ok {
			if c.err = c.touched[i].decode(v); c.err != nil {
				return
			}
		}
	}
}

func (c *notifyChunk) resolveFromTx(domains *execctx.SharedDomains, roTx kv.TemporalTx) error {
	if c.err != nil {
		return c.err
	}
	for i := range c.touched {
		if c.touched[i].resolved {
			continue
		}
		address := c.touched[i].addr.Value()
		v, _, err := domains.GetLatest(kv.AccountsDomain, roTx, address[:])
		if err != nil {
			return err
		}
		if err := c.touched[i].decode(v); err != nil {
			return err
		}
	}
	return nil
}

func (c *notifyChunk) replay(accumulator *shards.Accumulator) {
	for _, op := range c.ops {
		a := &c.touched[op.at]
		switch op.kind {
		case notifyCode:
			accumulator.ChangeCode(a.addr.Value(), a.incarnation, op.value)
		case notifyStorage:
			accumulator.ChangeStorage(a.addr.Value(), a.incarnation, op.key.Value(), op.value)
		case notifyAccount:
			accumulator.ChangeAccount(a.addr.Value(), a.incarnation, a.data)
		}
	}
}

func (a *notifiedAccount) decode(v []byte) error {
	acc := accounts.NewAccount()
	if len(v) > 0 {
		if err := accounts.DeserialiseV3(&acc, v); err != nil {
			return err
		}
	}
	a.incarnation, a.resolved = acc.Incarnation, true
	if a.changed {
		a.data = accounts.SerialiseV3(&acc)
	}
	return nil
}
