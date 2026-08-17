// Copyright 2024 The Erigon Authors
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
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
)

type StateV3 struct {
	domains                *execctx.SharedDomains
	logger                 log.Logger
	persistReceiptsCacheV2 bool
	txNum                  uint64
	trace                  atomic.Bool

	// Scratch for the per-transaction index writes; the apply goroutine alone touches them.
	receiptsWriter rawtemporaldb.ReceiptWriter
	traceAddr      common.Address
}

func NewStateV3(domains *execctx.SharedDomains, persistReceiptsCacheV2 bool, logger log.Logger) *StateV3 {
	return &StateV3{
		domains:                domains,
		logger:                 logger,
		persistReceiptsCacheV2: persistReceiptsCacheV2,
	}
}

func (rs *StateV3) SetTrace(trace bool) {
	rs.trace.Store(trace)
}

func (rs *StateV3) Domains() *execctx.SharedDomains {
	return rs.domains
}

func (rs *StateV3) SetTxNum(txNum uint64) {
	rs.txNum = txNum
}

func (writes *WriteSet) Apply(domains *execctx.SharedDomains, roTx kv.TemporalTx, blockNum, txNum uint64, balanceIncreases map[accounts.Address]uint256.Int, rules *chain.Rules, blockCache *BlockStateCache, trace bool) error {
	if writes != nil && !writes.IsEmpty() {
		if dbg.AssertEnabled {
			writes.assertSelfDestructNormalized()
		}
		// Has-flags avoid pointer fields, which would heap-escape once per field per address.
		type addrState struct {
			balance        uint256.Int
			nonce          uint64
			incarnation    uint64
			codeHash       accounts.CodeHash
			code           []byte
			hasBalance     bool
			hasNonce       bool
			hasIncarnation bool
			hasCodeHash    bool
			codeWritten    bool
			selfDestruct   bool
			createContract bool
			storage        []storageItem
		}

		perAddr := make(map[accounts.Address]*addrState)
		ensure := func(a accounts.Address) *addrState {
			d := perAddr[a]
			if d == nil {
				d = &addrState{}
				perAddr[a] = d
			}
			return d
		}
		for a, vw := range writes.Balances() {
			d := ensure(a)
			d.balance = vw.Val
			d.hasBalance = true
		}
		for a, vw := range writes.Nonces() {
			d := ensure(a)
			d.nonce = vw.Val
			d.hasNonce = true
		}
		for a, vw := range writes.Incarnations() {
			d := ensure(a)
			d.incarnation = vw.Val
			d.hasIncarnation = true
		}
		// CodeHashes must be applied before Codes: an explicit hash write wins over one derived from a Codes write.
		for a, vw := range writes.CodeHashes() {
			d := ensure(a)
			d.codeHash = vw.Val
			d.hasCodeHash = true
		}
		for a, vw := range writes.Codes() {
			d := ensure(a)
			d.code = vw.Val.Bytes
			d.codeWritten = true
		}
		for a, vw := range writes.SelfDestructs() {
			ensure(a).selfDestruct = vw.Val
		}
		for a, vw := range writes.createContract {
			ensure(a).createContract = vw.Val
		}
		for a, byKey := range writes.Storages() {
			d := ensure(a)
			for k, vw := range byKey {
				d.storage = append(d.storage, storageItem{k, vw.Val})
			}
		}

		// Sorted only for deterministic trace output; BTree buffering makes iteration order irrelevant to correctness.
		addrs := make([]accounts.Address, 0, len(perAddr))
		for addr := range perAddr {
			addrs = append(addrs, addr)
		}
		slices.SortFunc(addrs, func(a, b accounts.Address) int {
			av, bv := a.Value(), b.Value()
			return bytes.Compare(av[:], bv[:])
		})

		for _, addr := range addrs {
			d := perAddr[addr]
			address := addr.Value()

			if d.selfDestruct {
				if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d apply:del code+storage: %x\n", blockNum, addr)
				}
				// EIP-8246: a non-zero balance survives self-destruct, so this isn't a pure delete.
				sdPreservedBalance := d.hasBalance && !d.balance.IsZero()
				pureDelete := !sdPreservedBalance && !d.hasNonce && !d.hasIncarnation && !d.hasCodeHash
				if blockCache != nil {
					// Routed through the cache so a later SELFDESTRUCT in the same block supersedes
					// an earlier put; a direct domain delete would be overwritten by Flush's replay.
					blockCache.DeleteAccount(addr, txNum)
					if !domains.InlineTouchKeyDisabled() {
						domains.GetCommitmentContext().TouchKey(kv.AccountsDomain, string(address[:]), nil)
					}
					if pureDelete {
						if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
							fmt.Printf("%d apply:del account: %x\n", blockNum, addr)
						}
						continue
					}
				} else {
					if err := domains.DomainDel(kv.CodeDomain, roTx, address[:], txNum, nil); err != nil {
						return err
					}
					if err := domains.DomainDelPrefix(kv.StorageDomain, roTx, address[:], txNum); err != nil {
						return err
					}
					if pureDelete {
						if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
							fmt.Printf("%d apply:del account: %x\n", blockNum, addr)
						}
						if err := domains.DomainDel(kv.AccountsDomain, roTx, address[:], txNum, nil); err != nil {
							return err
						}
						continue
					}
				}
			}

			if d.createContract {
				if err := domains.DomainDelPrefix(kv.StorageDomain, roTx, address[:], txNum); err != nil {
					return err
				}
			}

			if d.hasBalance || d.hasNonce || d.hasIncarnation || d.hasCodeHash || d.codeWritten {
				// Self-destruct is the exception: its base stays empty so cleared fields aren't resurrected.
				acc := accounts.NewAccount()
				if !d.selfDestruct {
					if blockCache != nil {
						if enc, ok := blockCache.GetCurrentAccount(addr); ok && len(enc) > 0 {
							_ = accounts.DeserialiseV3(&acc, enc)
						} else if enc0, _, err := domains.GetLatest(kv.AccountsDomain, roTx, address[:]); err == nil && len(enc0) > 0 {
							_ = accounts.DeserialiseV3(&acc, enc0)
						}
					} else if enc0, _, err := domains.GetLatest(kv.AccountsDomain, roTx, address[:]); err == nil && len(enc0) > 0 {
						_ = accounts.DeserialiseV3(&acc, enc0)
					}
				}
				if d.hasBalance {
					acc.Balance = d.balance
				}
				if d.hasNonce {
					acc.Nonce = d.nonce
				}
				if d.hasIncarnation {
					acc.Incarnation = d.incarnation
				}
				if d.hasCodeHash {
					acc.CodeHash = d.codeHash
				} else if d.codeWritten {
					acc.CodeHash = accounts.NewCode(d.code).Hash
				}
				if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d apply:put account: %x balance:%s,nonce:%d,codehash:%x\n", blockNum, addr, acc.Balance.String(), acc.Nonce, acc.CodeHash)
				}
				enc := accounts.SerialiseV3(&acc)
				if blockCache != nil {
					blockCache.WriteAccount(addr, enc, txNum)
					if !domains.InlineTouchKeyDisabled() {
						domains.GetCommitmentContext().TouchKey(kv.AccountsDomain, string(address[:]), enc)
					}
				} else {
					if err := domains.DomainPut(kv.AccountsDomain, roTx, address[:], enc, txNum, nil); err != nil {
						return err
					}
				}
			}

			if d.codeWritten {
				if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
					code := d.code
					if len(code) > 40 {
						code = code[:40]
					}
					fmt.Printf("%d apply:put code: %x %x\n", blockNum, addr, code)
				}
				if blockCache != nil {
					blockCache.WriteCode(addr, d.code, txNum)
					if !domains.InlineTouchKeyDisabled() {
						domains.GetCommitmentContext().TouchKey(kv.CodeDomain, string(address[:]), d.code)
					}
				} else if len(d.code) == 0 {
					if err := domains.DomainDel(kv.CodeDomain, roTx, address[:], txNum, nil); err != nil {
						return err
					}
				} else if err := domains.DomainPut(kv.CodeDomain, roTx, address[:], d.code, txNum, nil); err != nil {
					return err
				}
			}

			for _, item := range d.storage {
				key := item.key.Value()
				composite := make([]byte, 0, len(address)+len(key))
				composite = append(composite, address[:]...)
				composite = append(composite, key[:]...)
				v := item.value.Bytes()
				if len(v) == 0 {
					if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d apply:del storage: %x %x\n", blockNum, addr, item.key)
					}
					if blockCache != nil {
						blockCache.WriteStorage(addr, item.key, nil, txNum)
						if !domains.InlineTouchKeyDisabled() {
							domains.GetCommitmentContext().TouchKey(kv.StorageDomain, string(composite), nil)
						}
					} else {
						if err := domains.DomainDel(kv.StorageDomain, roTx, composite, txNum, nil); err != nil {
							return err
						}
					}
				} else {
					if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d apply:put storage: %x %x %x\n", blockNum, addr, item.key, v)
					}
					if blockCache != nil {
						blockCache.WriteStorage(addr, item.key, v, txNum)
						if !domains.InlineTouchKeyDisabled() {
							domains.GetCommitmentContext().TouchKey(kv.StorageDomain, string(composite), v)
						}
					} else {
						if err := domains.DomainPut(kv.StorageDomain, roTx, composite, v, txNum, nil); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	var acc accounts.Account
	for addr, increase := range balanceIncreases {
		addrValue := addr.Value()
		var enc0 []byte
		if blockCache != nil {
			if enc, ok := blockCache.GetCurrentAccount(addr); ok {
				enc0 = enc
			} else {
				var err error
				enc0, _, err = domains.GetLatest(kv.AccountsDomain, roTx, addrValue[:])
				if err != nil {
					return err
				}
			}
		} else {
			var err error
			enc0, _, err = domains.GetLatest(kv.AccountsDomain, roTx, addrValue[:])
			if err != nil {
				return err
			}
		}
		acc.Reset()
		if len(enc0) > 0 {
			if err := accounts.DeserialiseV3(&acc, enc0); err != nil {
				return err
			}
		}
		acc.Balance.Add(&acc.Balance, &increase)
		if EIP161EmptyRemoval(rules.IsEIP161Enabled(), rules.IsAura, addr) && acc.Nonce == 0 && acc.Balance.IsZero() && acc.IsEmptyCodeHash() {
			if blockCache != nil {
				blockCache.DeleteAccount(addr, txNum)
				if !domains.InlineTouchKeyDisabled() {
					domains.GetCommitmentContext().TouchKey(kv.AccountsDomain, string(addrValue[:]), nil)
				}
			} else {
				if err := domains.DomainDel(kv.AccountsDomain, roTx, addrValue[:], txNum, enc0); err != nil {
					return err
				}
			}
		} else {
			enc1 := accounts.SerialiseV3(&acc)
			if blockCache != nil {
				blockCache.WriteAccount(addr, enc1, txNum)
				if !domains.InlineTouchKeyDisabled() {
					domains.GetCommitmentContext().TouchKey(kv.AccountsDomain, string(addrValue[:]), enc1)
				}
			} else {
				if err := domains.DomainPut(kv.AccountsDomain, roTx, addrValue[:], enc1, txNum, enc0); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// With blockCache set, writes land in the cache and flush to SharedDomains at
// the block boundary instead of going there directly.
func (rs *StateV3) ApplyStateWrites(_ context.Context,
	roTx kv.TemporalTx,
	blockNum uint64,
	txNum uint64,
	writes *WriteSet,
	balanceIncreases map[accounts.Address]uint256.Int,
	rules *chain.Rules,
	blockCache *BlockStateCache,
) error {
	if writes.IsEmpty() && len(balanceIncreases) == 0 {
		return nil
	}
	if err := writes.Apply(rs.domains, roTx, blockNum, txNum, balanceIncreases, rules, blockCache, rs.trace.Load()); err != nil {
		return fmt.Errorf("StateV3.ApplyStateWrites: %w", err)
	}
	return nil
}

// skipReceiptCache avoids a second write at the shared system-tx-end txNum overwriting the last regular tx's receipt history.
func (rs *StateV3) ApplyTxIndexes(
	roTx kv.TemporalTx,
	txNum uint64,
	receipt *types.Receipt,
	cummulativeBlobGas uint64,
	logs []*types.Log,
	traceFroms map[accounts.Address]struct{},
	traceTos map[accounts.Address]struct{},
	skipReceiptCache ...bool,
) error {
	skip := len(skipReceiptCache) > 0 && skipReceiptCache[0]
	if err := rs.applyLogsAndTraces4(roTx, txNum, receipt, cummulativeBlobGas, logs, traceFroms, traceTos, false, skip); err != nil {
		return fmt.Errorf("StateV3.ApplyTxIndexes: %w", err)
	}
	return nil
}

func (rs *StateV3) CommitStepBoundary(ctx context.Context, roTx kv.TemporalTx, blockNum, txNum uint64) error {
	if rs.domains.IsUnfrozenStepEdge(roTx, txNum) && !rs.domains.InlineTouchKeyDisabled() {
		_, err := rs.domains.ComputeCommitment(ctx, roTx, true, blockNum, txNum,
			fmt.Sprintf("applying step %d", txNum/rs.domains.StepSize()), nil)
		if err != nil {
			return fmt.Errorf("StateV3.CommitStepBoundary: %w", err)
		}
	}
	return nil
}

func (rs *StateV3) applyLogsAndTraces4(tx kv.TemporalTx, txNum uint64, receipt *types.Receipt, cummulativeBlobGas uint64, logs []*types.Log, traceFroms map[accounts.Address]struct{}, traceTos map[accounts.Address]struct{}, historyExecution bool, skipReceiptCache bool) error {
	domains := rs.domains
	for addr := range traceFroms {
		rs.traceAddr = addr.Value()
		if err := domains.IndexAdd(kv.TracesFromIdx, rs.traceAddr[:], txNum); err != nil {
			return err
		}
	}

	for addr := range traceTos {
		rs.traceAddr = addr.Value()
		if err := domains.IndexAdd(kv.TracesToIdx, rs.traceAddr[:], txNum); err != nil {
			return err
		}
	}

	for _, lg := range logs {
		if err := domains.IndexAdd(kv.LogAddrIdx, lg.Address[:], txNum); err != nil {
			return err
		}
		for i := range lg.Topics {
			if err := domains.IndexAdd(kv.LogTopicIdx, lg.Topics[i][:], txNum); err != nil {
				return err
			}
		}
	}

	var putter kv.TemporalPutDel

	if receipt != nil {
		if !historyExecution {
			blockLogIndex := receipt.FirstLogIndexWithinBlock
			if !rawtemporaldb.ReceiptStoresFirstLogIdx(tx) {
				blockLogIndex += uint32(len(receipt.Logs))
			}
			putter = domains.AsPutDel(tx)
			if err := rs.receiptsWriter.AppendMetadata(putter, blockLogIndex, receipt.CumulativeGasUsed, cummulativeBlobGas, txNum); err != nil {
				return err
			}
		}
	}

	if rs.persistReceiptsCacheV2 && !skipReceiptCache {
		if putter == nil {
			putter = domains.AsPutDel(tx)
		}
		if err := rs.receiptsWriter.Append(putter, receipt, txNum); err != nil {
			return err
		}
	}

	return nil
}

func (rs *StateV3) SizeEstimateBeforeCommitment() uint64 {
	if rs.domains == nil {
		return 0
	}
	sz := rs.domains.Size()
	sz *= 2 // for Commitment calculation when batch is full
	return sz
}

func (rs *StateV3) SizeEstimateAfterCommitment() uint64 {
	if rs.domains == nil {
		return 0
	}
	return rs.domains.Size()
}

type storageItem struct {
	key   accounts.StorageKey
	value uint256.Int
}

var deleted accounts.Account

type bufferedAccount struct {
	data       *accounts.Account
	code       []byte
	storage    *btree.BTreeG[storageItem]
	wasDeleted bool // set when DeleteAccount was called; survives UpdateAccountCode overwrite
}

type StateV3Buffered struct {
	*StateV3
	accounts      map[accounts.Address]*bufferedAccount
	accountsMutex *sync.RWMutex
}

func NewStateV3Buffered(state *StateV3) *StateV3Buffered {
	bufferedState := &StateV3Buffered{
		StateV3:       state,
		accounts:      map[accounts.Address]*bufferedAccount{},
		accountsMutex: &sync.RWMutex{},
	}
	return bufferedState
}

// ClearAccountsCache must run after a block's writes are applied to SharedDomains
// and before the next block starts reading, or workers see stale cached values.
func (s *StateV3Buffered) ClearAccountsCache() {
	s.accountsMutex.Lock()
	clear(s.accounts)
	s.accountsMutex.Unlock()
}

func (s *StateV3Buffered) WithDomains(domains *execctx.SharedDomains) *StateV3Buffered {
	return &StateV3Buffered{
		StateV3:       NewStateV3(domains, s.persistReceiptsCacheV2, s.logger),
		accounts:      s.accounts,
		accountsMutex: s.accountsMutex,
	}
}

// versionedWriteCollector buffers StateWriter calls into a WriteSet and mirrors
// them into rs.accounts, so block N+1 workers can see block N before the async flush.
type versionedWriteCollector struct {
	rs     *StateV3Buffered
	writes *WriteSet
}

func NewVersionedWriteCollector(rs *StateV3Buffered) *versionedWriteCollector {
	return &versionedWriteCollector{rs: rs, writes: &WriteSet{}}
}

func (c *versionedWriteCollector) Writes() *WriteSet { return c.writes }

func (c *versionedWriteCollector) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	// Copy to avoid aliasing a pooled stateObject, which may be overwritten after tx finalization.
	var accountCopy accounts.Account
	accountCopy.Copy(account)
	accountCopy.PrevIncarnation = account.PrevIncarnation

	// A higher original incarnation than the new one means destroy-then-recreate:
	// emit SelfDestructPath first so applyVersionedWrites treats it as cleanup, not deletion.
	needsCleanup := original.Incarnation > accountCopy.Incarnation
	// Cross-block case: a prior-block self-destruct reads back as Incarnation=0, which
	// the check above misses; rs.accounts still carries that block's deleted marker.
	if !needsCleanup && accountCopy.Incarnation > 0 {
		c.rs.accountsMutex.RLock()
		if obj, ok := c.rs.accounts[address]; ok && obj.wasDeleted {
			needsCleanup = true
		}
		c.rs.accountsMutex.RUnlock()
	}
	if needsCleanup {
		c.writes.SetSelfDestruct(address, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: address, Path: SelfDestructPath}, Val: true})
	}

	c.writes.SetBalance(address, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: address, Path: BalancePath}, Val: accountCopy.Balance})
	c.writes.SetNonce(address, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: address, Path: NoncePath}, Val: accountCopy.Nonce})
	c.writes.SetIncarnation(address, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: address, Path: IncarnationPath}, Val: accountCopy.Incarnation})
	c.writes.SetCodeHash(address, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: address, Path: CodeHashPath}, Val: accountCopy.CodeHash})

	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok || obj.data == &deleted {
		wasDel := ok && (obj.data == &deleted || obj.wasDeleted)
		obj = &bufferedAccount{wasDeleted: wasDel}
	}
	obj.data = &accountCopy
	c.rs.accounts[address] = obj
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	c.writes.SetCode(address, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: address, Path: CodePath}, Val: accounts.Code{Hash: codeHash, Bytes: code}})

	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok || obj.data == &deleted {
		wasDel := ok && obj.data == &deleted
		obj = &bufferedAccount{wasDeleted: wasDel}
		c.rs.accounts[address] = obj
	}
	obj.code = code
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	c.writes.SetSelfDestruct(address, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: address, Path: SelfDestructPath}, Val: true})

	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{data: &deleted, wasDeleted: true}
		c.rs.accounts[address] = obj
	}
	*obj = bufferedAccount{data: &deleted, wasDeleted: true}
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	c.writes.SetStorage(address, key, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: address, Path: StoragePath, Key: key}, Val: value})

	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok || obj.data == &deleted {
		obj = &bufferedAccount{}
		c.rs.accounts[address] = obj
	}
	if obj.storage == nil {
		obj.storage = btree.NewBTreeGOptions[storageItem](func(a, b storageItem) bool {
			return a.key.Cmp(b.key) > 0
		}, btree.Options{NoLocks: true})
	}
	obj.storage.Set(storageItem{key, value})
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) CreateContract(_ accounts.Address) error { return nil }

// Caller must call StartChange on accumulator before invoking this.
func NotifyAccumulator(accumulator *shards.Accumulator, writes *WriteSet) {
	if accumulator == nil || writes.IsEmpty() {
		return
	}

	type pendingAccount struct {
		balance     *uint256.Int
		nonce       *uint64
		incarnation *uint64
		codeHash    *accounts.CodeHash
	}

	pending := make(map[accounts.Address]*pendingAccount, writes.Count()/4+1)
	get := func(addr accounts.Address) *pendingAccount {
		p := pending[addr]
		if p == nil {
			p = &pendingAccount{}
			pending[addr] = p
		}
		return p
	}

	for addr, w := range writes.Balances() {
		v := w.Val
		get(addr).balance = &v
	}
	for addr, w := range writes.Nonces() {
		v := w.Val
		get(addr).nonce = &v
	}
	for addr, w := range writes.Incarnations() {
		v := w.Val
		get(addr).incarnation = &v
	}
	for addr, w := range writes.CodeHashes() {
		v := w.Val
		get(addr).codeHash = &v
	}
	for addr, w := range writes.Codes() {
		var inc uint64
		if p := pending[addr]; p != nil && p.incarnation != nil {
			inc = *p.incarnation
		}
		accumulator.ChangeCode(addr.Value(), inc, w.Val.Bytes)
	}
	for addr, byKey := range writes.Storages() {
		var inc uint64
		if p := pending[addr]; p != nil && p.incarnation != nil {
			inc = *p.incarnation
		}
		for key, w := range byKey {
			val := w.Val
			accumulator.ChangeStorage(addr.Value(), inc, key.Value(), val.Bytes())
		}
	}

	for addr, p := range pending {
		// UpdateAccountData always sets balance and nonce together, so this pair alone detects "no account write".
		if p.balance == nil && p.nonce == nil {
			continue
		}
		var acc accounts.Account
		if p.balance != nil {
			acc.Balance = *p.balance
		}
		if p.nonce != nil {
			acc.Nonce = *p.nonce
		}
		if p.incarnation != nil {
			acc.Incarnation = *p.incarnation
		}
		if p.codeHash != nil {
			acc.CodeHash = *p.codeHash
		}
		serialised := accounts.SerialiseV3(&acc)
		accumulator.ChangeAccount(addr.Value(), acc.Incarnation, serialised)
	}
}

// Writer accumulates per-worker updates for later conflict-resolution.
type Writer struct {
	tx          kv.TemporalPutDel
	trace       bool
	accumulator *shards.Accumulator
	txNum       uint64
}

func NewWriter(tx kv.TemporalPutDel, accumulator *shards.Accumulator, txNum uint64) *Writer {
	return &Writer{
		tx:          tx,
		accumulator: accumulator,
		txNum:       txNum,
	}
}

func (w *Writer) SetTxNum(v uint64)              { w.txNum = v }
func (w *Writer) SetPutDel(tx kv.TemporalPutDel) { w.tx = tx }

func (w *Writer) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return nil, nil, nil, nil
}

func (w *Writer) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("Writer: acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	addressValue := address.Value()
	if original.Incarnation > account.Incarnation {
		if err := w.tx.DomainDel(kv.CodeDomain, addressValue[:], w.txNum, nil); err != nil {
			return err
		}
		if err := w.tx.DomainDelPrefix(kv.StorageDomain, addressValue[:], w.txNum); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(addressValue, account.Incarnation, value)
	}

	if err := w.tx.DomainPut(kv.AccountsDomain, addressValue[:], value, w.txNum, nil); err != nil {
		return err
	}
	return nil
}

func (w *Writer) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address, codeHash, len(code))
	}
	addressValue := address.Value()
	if len(code) == 0 {
		if err := w.tx.DomainDel(kv.CodeDomain, addressValue[:], w.txNum, nil); err != nil {
			return err
		}
	} else if err := w.tx.DomainPut(kv.CodeDomain, addressValue[:], code, w.txNum, nil); err != nil {
		return err
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(addressValue, incarnation, code)
	}
	return nil
}

func (w *Writer) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	addressValue := address.Value()
	if err := w.tx.DomainDel(kv.AccountsDomain, addressValue[:], w.txNum, nil); err != nil {
		return err
	}
	return nil
}

func (w *Writer) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	var addressValue common.Address
	if !address.IsNil() {
		addressValue = address.Value()
	}
	var keyValue common.Hash
	if !key.IsNil() {
		keyValue = key.Value()
	}
	composite := make([]byte, 0, len(addressValue)+len(keyValue))
	composite = append(composite, addressValue[:]...)
	composite = append(composite, keyValue[:]...)
	v := value.Bytes()
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, v)
	}
	if len(v) == 0 {
		return w.tx.DomainDel(kv.StorageDomain, composite, w.txNum, nil)
	}
	if w.accumulator != nil {
		w.accumulator.ChangeStorage(addressValue, incarnation, keyValue, v)
	}

	return w.tx.DomainPut(kv.StorageDomain, composite, v, w.txNum, nil)
}

var fastCreate = dbg.EnvBool("FAST_CREATE", false)

func (w *Writer) CreateContract(address accounts.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	if fastCreate {
		return nil
	}
	addressValue := address.Value()
	if err := w.tx.DomainDelPrefix(kv.StorageDomain, addressValue[:], w.txNum); err != nil {
		return err
	}
	return nil
}

// ReaderV3 is not thread-safe.
type ReaderV3 struct {
	txNum       uint64
	trace       bool
	tracePrefix string
	getter      kv.TemporalGetter

	addr      common.Address
	composite [length.Addr + length.Hash]byte // addr||slot, reused across calls to avoid per-call allocation
}

func NewReaderV3(getter kv.TemporalGetter) *ReaderV3 {
	return &ReaderV3{
		getter: getter,
	}
}

func (r *ReaderV3) DiscardReadList()                   {}
func (r *ReaderV3) SetTxNum(txNum uint64)              { r.txNum = txNum }
func (r *ReaderV3) SetGetter(getter kv.TemporalGetter) { r.getter = getter }

func (r *ReaderV3) SetTrace(trace bool, tracePrefix string) {
	r.trace = trace
	if tplen := len(tracePrefix); tplen > 0 && tracePrefix[tplen-1] != ' ' {
		tracePrefix += " "
	}
	r.tracePrefix = tracePrefix
}

func (r *ReaderV3) Trace() bool {
	return r.trace
}

func (r *ReaderV3) TracePrefix() string {
	return r.tracePrefix
}

// BlockStateCache buffers per-block state for the parallel executor: a
// lazily-populated pre-block read cache, plus a write buffer Flush drains to SharedDomains at the block boundary.
// Thread-safe: the block's worker goroutines all share one cache.
type BlockStateCache struct {
	mu sync.RWMutex

	committedAccounts sync.Map // accounts.Address -> *accounts.Account (nil ptr = absent)
	committedStorage  sync.Map // committedStorageKey -> []byte (nil slice = cached empty slot)

	currentAccounts map[accounts.Address][]byte
	currentStorage  map[accounts.Address]map[accounts.StorageKey][]byte

	currentCode map[accounts.Address][]byte

	// writeLog stamps each write with its per-tx txNum; Flush replays at that txNum
	// so per-tx domain history matches the serial executor instead of collapsing into one write.
	writeLog []bcWriteOp
}

type committedStorageKey struct {
	addr accounts.Address
	key  accounts.StorageKey
}

type bcOpKind uint8

const (
	bcOpPutAccount    bcOpKind = iota + 1
	bcOpPutCode                // val=nil means code delete
	bcOpPutStorage             // val=nil means storage delete
	bcOpDeleteAccount          // self-destruct / empty-removal: del code + del storage prefix
)

type bcWriteOp struct {
	kind  bcOpKind
	addr  accounts.Address
	key   accounts.StorageKey
	val   []byte
	txNum uint64
}

func NewBlockStateCache() *BlockStateCache {
	return &BlockStateCache{
		currentAccounts: make(map[accounts.Address][]byte),
		currentStorage:  make(map[accounts.Address]map[accounts.StorageKey][]byte),
		currentCode:     make(map[accounts.Address][]byte),
	}
}

func (c *BlockStateCache) GetCommittedAccount(addr accounts.Address) (*accounts.Account, bool) {
	v, ok := c.committedAccounts.Load(addr)
	if !ok {
		return nil, false
	}
	return v.(*accounts.Account), true
}

// PutCommittedAccount caches a pre-block account. Nil = doesn't exist.
func (c *BlockStateCache) PutCommittedAccount(addr accounts.Address, acc *accounts.Account) {
	c.committedAccounts.Store(addr, acc)
}

func (c *BlockStateCache) GetCommittedStorage(addr accounts.Address, key accounts.StorageKey) ([]byte, bool) {
	v, ok := c.committedStorage.Load(committedStorageKey{addr: addr, key: key})
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}

// PutCommittedStorage caches a pre-block storage value. nil = empty slot.
func (c *BlockStateCache) PutCommittedStorage(addr accounts.Address, key accounts.StorageKey, val []byte) {
	c.committedStorage.Store(committedStorageKey{addr: addr, key: key}, val)
}

func (c *BlockStateCache) WriteAccount(addr accounts.Address, enc []byte, txNum uint64) {
	c.mu.Lock()
	c.currentAccounts[addr] = enc
	c.writeLog = append(c.writeLog, bcWriteOp{kind: bcOpPutAccount, addr: addr, val: enc, txNum: txNum})
	c.mu.Unlock()
}

func (c *BlockStateCache) WriteStorage(addr accounts.Address, key accounts.StorageKey, val []byte, txNum uint64) {
	c.mu.Lock()
	slots, ok := c.currentStorage[addr]
	if !ok {
		slots = make(map[accounts.StorageKey][]byte)
		c.currentStorage[addr] = slots
	}
	slots[key] = val
	c.writeLog = append(c.writeLog, bcWriteOp{kind: bcOpPutStorage, addr: addr, key: key, val: val, txNum: txNum})
	c.mu.Unlock()
}

func (c *BlockStateCache) WriteCode(addr accounts.Address, code []byte, txNum uint64) {
	c.mu.Lock()
	c.currentCode[addr] = code
	c.writeLog = append(c.writeLog, bcWriteOp{kind: bcOpPutCode, addr: addr, val: code, txNum: txNum})
	c.mu.Unlock()
}

func (c *BlockStateCache) DeleteAccount(addr accounts.Address, txNum uint64) {
	c.mu.Lock()
	// nil (not absent) marks deletion, so GetCurrentAccount reports "present but
	// empty" instead of falling back to committed state.
	if v, present := c.currentAccounts[addr]; present && v == nil {
		// Already deleted earlier in this block; skip so Flush emits one DomainDel.
		c.mu.Unlock()
		return
	}
	c.currentAccounts[addr] = nil
	delete(c.currentCode, addr)
	delete(c.currentStorage, addr)
	c.writeLog = append(c.writeLog, bcWriteOp{kind: bcOpDeleteAccount, addr: addr, txNum: txNum})
	c.mu.Unlock()
}

func (c *BlockStateCache) GetCurrentAccountDecoded(addr accounts.Address) (*accounts.Account, bool, error) {
	c.mu.RLock()
	enc, written := c.currentAccounts[addr]
	c.mu.RUnlock()
	if written {
		if enc == nil {
			return nil, true, nil
		}
		acc := new(accounts.Account)
		if err := accounts.DeserialiseV3(acc, enc); err != nil {
			return nil, true, err
		}
		return acc, true, nil
	}
	if acc, ok := c.GetCommittedAccount(addr); ok {
		if acc == nil {
			return nil, true, nil
		}
		result := *acc
		return &result, true, nil
	}
	return nil, false, nil
}

func (c *BlockStateCache) GetCurrentAccount(addr accounts.Address) ([]byte, bool) {
	c.mu.RLock()
	if enc, ok := c.currentAccounts[addr]; ok {
		c.mu.RUnlock()
		return enc, true
	}
	c.mu.RUnlock()
	// Reading committed after releasing mu is safe: committedAccounts is a write-once
	// immutable view, so a concurrent write can only add an entry we'd miss, never tear a value.
	if v, ok := c.committedAccounts.Load(addr); ok {
		acc := v.(*accounts.Account)
		if acc == nil {
			return nil, true
		}
		return accounts.SerialiseV3(acc), true
	}
	return nil, false
}

func (c *BlockStateCache) GetCurrentStorage(addr accounts.Address, key accounts.StorageKey) ([]byte, bool) {
	c.mu.RLock()
	if slots, ok := c.currentStorage[addr]; ok {
		if val, ok := slots[key]; ok {
			c.mu.RUnlock()
			return val, true
		}
	}
	c.mu.RUnlock()
	if v, ok := c.committedStorage.Load(committedStorageKey{addr: addr, key: key}); ok {
		return v.([]byte), true
	}
	return nil, false
}

func (c *BlockStateCache) GetCurrentCode(addr accounts.Address) ([]byte, bool) {
	c.mu.RLock()
	if code, ok := c.currentCode[addr]; ok {
		c.mu.RUnlock()
		return code, true
	}
	c.mu.RUnlock()
	return nil, false
}

// Flush replays writeLog into SharedDomains at each entry's txNum. DomainPut/DomainDel
// no-op on a value that already matches sd.mem, so logging every Write* call is safe.
func (c *BlockStateCache) Flush(domains *execctx.SharedDomains, roTx kv.TemporalTx) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := range c.writeLog {
		op := &c.writeLog[i]
		addrVal := op.addr.Value()
		switch op.kind {
		case bcOpDeleteAccount:
			if err := domains.DomainDel(kv.AccountsDomain, roTx, addrVal[:], op.txNum, nil); err != nil {
				return err
			}
			if err := domains.DomainDel(kv.CodeDomain, roTx, addrVal[:], op.txNum, nil); err != nil {
				return err
			}
			if err := domains.DomainDelPrefix(kv.StorageDomain, roTx, addrVal[:], op.txNum); err != nil {
				return err
			}
		case bcOpPutAccount:
			if err := domains.DomainPut(kv.AccountsDomain, roTx, addrVal[:], op.val, op.txNum, nil); err != nil {
				return err
			}
		case bcOpPutCode:
			if len(op.val) == 0 {
				if err := domains.DomainDel(kv.CodeDomain, roTx, addrVal[:], op.txNum, nil); err != nil {
					return err
				}
			} else if err := domains.DomainPut(kv.CodeDomain, roTx, addrVal[:], op.val, op.txNum, nil); err != nil {
				return err
			}
		case bcOpPutStorage:
			keyVal := op.key.Value()
			composite := make([]byte, 20+32)
			copy(composite, addrVal[:])
			copy(composite[20:], keyVal[:])
			if len(op.val) == 0 {
				if err := domains.DomainDel(kv.StorageDomain, roTx, composite, op.txNum, nil); err != nil {
					return err
				}
			} else {
				if err := domains.DomainPut(kv.StorageDomain, roTx, composite, op.val, op.txNum, nil); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

type CachedReaderV3 struct {
	*ReaderV3
	blockCache  *BlockStateCache
	readCurrent bool // when true, read from currentAccounts (post-TX) instead of committedAccounts (pre-block)
}

func NewCachedReaderV3(getter kv.TemporalGetter, blockCache *BlockStateCache) *CachedReaderV3 {
	return &CachedReaderV3{
		ReaderV3:   NewReaderV3(getter),
		blockCache: blockCache,
	}
}

// NewCurrentCachedReaderV3 reads the in-block write buffer first, for callers needing post-TX state.
func NewCurrentCachedReaderV3(getter kv.TemporalGetter, blockCache *BlockStateCache) *CachedReaderV3 {
	return &CachedReaderV3{
		ReaderV3:    NewReaderV3(getter),
		blockCache:  blockCache,
		readCurrent: true,
	}
}

func (r *CachedReaderV3) SetBlockStateCache(cache *BlockStateCache) {
	r.blockCache = cache
}

func (r *CachedReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	if r.blockCache != nil {
		if r.readCurrent {
			acc, ok, err := r.blockCache.GetCurrentAccountDecoded(address)
			if err != nil {
				return nil, err
			}
			if ok {
				return acc, nil
			}
		} else {
			if acc, ok := r.blockCache.GetCommittedAccount(address); ok {
				if acc == nil {
					return nil, nil
				}
				result := *acc
				return &result, nil
			}
		}
	}
	acc, err := r.ReaderV3.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if r.blockCache != nil {
		r.blockCache.PutCommittedAccount(address, acc)
	}
	if acc != nil {
		result := *acc
		return &result, nil
	}
	return nil, nil
}

func (r *CachedReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	if r.blockCache != nil && r.readCurrent {
		if code, ok := r.blockCache.GetCurrentCode(address); ok {
			return code, nil
		}
	}
	return r.ReaderV3.ReadAccountCode(address)
}

func (r *CachedReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	if r.blockCache != nil && r.readCurrent {
		if code, ok := r.blockCache.GetCurrentCode(address); ok {
			return len(code), nil
		}
	}
	return r.ReaderV3.ReadAccountCodeSize(address)
}

func (r *CachedReaderV3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if r.blockCache != nil {
		if r.readCurrent {
			if val, ok := r.blockCache.GetCurrentStorage(address, key); ok {
				var v uint256.Int
				if len(val) > 0 {
					v.SetBytes(val)
				}
				return v, len(val) > 0, nil
			}
		}
		if val, ok := r.blockCache.GetCommittedStorage(address, key); ok {
			var v uint256.Int
			if len(val) > 0 {
				v.SetBytes(val)
			}
			return v, len(val) > 0, nil
		}
	}
	v, ok, err := r.ReaderV3.ReadAccountStorage(address, key)
	if err != nil {
		return v, ok, err
	}
	if r.blockCache != nil {
		if ok {
			r.blockCache.PutCommittedStorage(address, key, v.Bytes())
		} else {
			r.blockCache.PutCommittedStorage(address, key, nil)
		}
	}
	return v, ok, nil
}

func (r *ReaderV3) HasStorage(address accounts.Address) (bool, error) {
	r.addr = address.Value()
	// Checking AccountsDomain first also catches an unwind-deleted account before trusting stale storage.
	if enc, _, err := r.getter.GetLatest(kv.AccountsDomain, r.addr[:]); len(enc) == 0 {
		return false, err
	}
	_, _, hasStorage, err := r.getter.HasPrefix(kv.StorageDomain, r.addr[:])
	return hasStorage, err
}

func (r *ReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	_, acc, err := r.readAccountData(address)
	return acc, err
}

func (r *ReaderV3) readAccountData(address accounts.Address) ([]byte, *accounts.Account, error) {
	r.addr = address.Value()
	enc, _, err := r.getter.GetLatest(kv.AccountsDomain, r.addr[:])
	if err != nil {
		return nil, nil, err
	}
	if len(enc) == 0 {
		if r.trace {
			fmt.Printf("%sReadAccountData [%x] => [empty], txNum: %d\n", r.tracePrefix, address, r.txNum)
		}
		return nil, nil, nil
	}

	var acc accounts.Account
	if err := accounts.DeserialiseV3(&acc, enc); err != nil {
		return nil, nil, err
	}
	if r.trace {
		fmt.Printf("%sReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], txNum: %d\n", r.tracePrefix, address, acc.Nonce, &acc.Balance, acc.CodeHash, r.txNum)
	}
	return enc, &acc, nil
}

func (r *ReaderV3) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *ReaderV3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	keyValue := key.Value()
	copy(r.composite[:length.Addr], addressValue[:])
	copy(r.composite[length.Addr:], keyValue[:])
	enc, _, err := r.getter.GetLatest(kv.StorageDomain, r.composite[:])
	if err != nil {
		return uint256.Int{}, false, err
	}

	ok := enc != nil
	var res uint256.Int
	if ok {
		(&res).SetBytes(enc)
	}

	if r.trace {
		r.traceReadAccountStorage(address, key, enc, res)
	}

	return res, ok, err
}

// traceReadAccountStorage is split out (and takes res by value) so the &res it
// needs for %x formatting does not force res to the heap on every read.
//
//go:noinline
func (r *ReaderV3) traceReadAccountStorage(address accounts.Address, key accounts.StorageKey, enc []byte, res uint256.Int) {
	if enc == nil {
		fmt.Printf("%sReadAccountStorage [%x %x] => [empty], txNum: %d, stack: %s\n", r.tracePrefix, address, key, r.txNum, dbg.Stack())
	} else {
		fmt.Printf("%sReadAccountStorage [%x %x] => [%x], txNum: %d, stack: %s\n", r.tracePrefix, address, key, &res, r.txNum, dbg.Stack())
	}
}

func (r *ReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	r.addr = address.Value()
	var enc []byte
	var err error
	if cg, ok := r.getter.(codeGetter); ok {
		enc, _, err = cg.GetCode(r.addr[:], r.txNum)
	} else {
		enc, _, err = r.getter.GetLatest(kv.CodeDomain, r.addr[:])
	}
	if err != nil {
		return nil, err
	}
	if r.trace {
		lenc, cs := printCode(enc)
		fmt.Printf("%sReadAccountCode [%x] =>  [%d:%s], txNum: %d, stack: %s\n", r.tracePrefix, address, lenc, cs, r.txNum, dbg.Stack())
	}
	return enc, nil
}

// codeGetter is the fast-path interface for full-code reads. Read-only: never used
// to resolve a DomainPut prevVal, unlike the setter path's GetLatest.
type codeGetter interface {
	GetCode(addr []byte, txNum uint64) ([]byte, bool, error)
}

type codeSizeGetter interface {
	GetCodeSize(addr []byte, txNum uint64) (int, bool, error)
}

func (r *ReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	r.addr = address.Value()
	if sg, ok := r.getter.(codeSizeGetter); ok {
		size, _, err := sg.GetCodeSize(r.addr[:], r.txNum)
		if err != nil {
			return 0, err
		}
		if r.trace {
			fmt.Printf("%sReadAccountCodeSize (sz) [%x] => [%d], txNum: %d\n", r.tracePrefix, r.addr, size, r.txNum)
		}
		return size, nil
	}
	enc, _, err := r.getter.GetLatest(kv.CodeDomain, r.addr[:])
	if err != nil {
		return 0, err
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("%sReadAccountCodeSize [%x] => [%d], txNum: %d\n", r.tracePrefix, r.addr, size, r.txNum)
	}
	return size, nil
}

func (r *ReaderV3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}

type bufferedReader struct {
	reader        StateReader
	bufferedState *StateV3Buffered
}

type latestBufferedReader struct {
	bufferedReader
}

func (r *latestBufferedReader) SetGetter(getter kv.TemporalGetter) {
	r.reader.(interface{ SetGetter(kv.TemporalGetter) }).SetGetter(getter)
}

type historicBufferedReader struct {
	bufferedReader
}

func (r *historicBufferedReader) SetTx(tx kv.TemporalTx) {
	r.reader.(interface{ SetTx(kv.TemporalTx) }).SetTx(tx)
}

func NewBufferedReader(bufferedState *StateV3Buffered, reader StateReader) StateReader {
	type latest interface {
		SetGetter(kv.TemporalGetter)
	}

	type historic interface {
		SetTx(kv.TemporalTx)
	}
	switch reader.(type) {
	case latest:
		return &latestBufferedReader{bufferedReader{reader: reader, bufferedState: bufferedState}}
	case historic:
		return &historicBufferedReader{bufferedReader{reader: reader, bufferedState: bufferedState}}
	default:
		return &bufferedReader{reader: reader, bufferedState: bufferedState}
	}
}

func (r *bufferedReader) SetTrace(trace bool, tracePrefix string) {
	r.reader.SetTrace(trace, tracePrefix)
}

func (r *bufferedReader) Trace() bool {
	return r.reader.Trace()
}

func (r *bufferedReader) TracePrefix() string {
	return r.reader.TracePrefix()
}

func (r *bufferedReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		if data == &deleted {
			if r.reader.Trace() {
				fmt.Printf("%sReadAccountData (buf)[%x] => [empty]\n", r.reader.TracePrefix(), address)
			}
			return nil, nil
		}
		if r.reader.Trace() {
			fmt.Printf("%sReadAccountData (buf)[%x] => [nonce: %d, balance: %d, codeHash: %x]\n", r.reader.TracePrefix(), address, data.Nonce, &data.Balance, data.CodeHash)
		}

		result := *data
		return &result, nil
	}

	return r.reader.ReadAccountData(address)
}

func (r *bufferedReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	var data *accounts.Account

	r.bufferedState.accountsMutex.RLock()
	if so, ok := r.bufferedState.accounts[address]; ok {
		data = so.data
	}
	r.bufferedState.accountsMutex.RUnlock()

	if data != nil {
		if data == &deleted {
			return nil, nil
		}
		result := *data
		return &result, nil
	}

	return r.reader.ReadAccountDataForDebug(address)
}

func (r *bufferedReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]

	if ok {
		if so.data == &deleted {
			if r.reader.Trace() {
				fmt.Printf("%sReadAccountStorage (buf)[%x %x] => [empty]\n", r.reader.TracePrefix(), address, key)
			}
			r.bufferedState.accountsMutex.RUnlock()
			return uint256.Int{}, false, nil
		}

		if so.storage != nil {
			item, ok := so.storage.Get(storageItem{key: key})

			if ok {
				if r.reader.Trace() {
					fmt.Printf("%sReadAccountStorage (buf)[%x %x] => [%x]\n", r.reader.TracePrefix(), address, key, &item.value)
				}
				r.bufferedState.accountsMutex.RUnlock()
				return item.value, true, nil
			}
		}
	}

	r.bufferedState.accountsMutex.RUnlock()

	return r.reader.ReadAccountStorage(address, key)
}

func (r *bufferedReader) HasStorage(address accounts.Address) (bool, error) {
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]

	if ok {
		if so.data == &deleted {
			r.bufferedState.accountsMutex.RUnlock()
			return false, nil
		}

		if so.storage != nil && so.storage.Len() > 0 {
			r.bufferedState.accountsMutex.RUnlock()
			return true, nil
		}
	}
	r.bufferedState.accountsMutex.RUnlock()
	return r.reader.HasStorage(address)
}

func (r *bufferedReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	var code []byte
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok {
		if so.data == &deleted {
			r.bufferedState.accountsMutex.RUnlock()
			return nil, nil
		}

		if len(so.code) != 0 {
			code = so.code
		}
	}
	r.bufferedState.accountsMutex.RUnlock()

	if len(code) != 0 {
		return code, nil
	}

	return r.reader.ReadAccountCode(address)
}

func (r *bufferedReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	var code []byte
	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok {
		if so.data == &deleted {
			r.bufferedState.accountsMutex.RUnlock()
			return 0, nil
		}

		if len(so.code) != 0 {
			code = so.code
		}
	}

	r.bufferedState.accountsMutex.RUnlock()

	if len(code) != 0 {
		return len(code), nil
	}

	return r.reader.ReadAccountCodeSize(address)
}

func (r *bufferedReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	var incarnation uint64

	r.bufferedState.accountsMutex.RLock()
	so, ok := r.bufferedState.accounts[address]
	if ok && so.data != nil {
		incarnation = so.data.Incarnation
	}
	r.bufferedState.accountsMutex.RUnlock()

	if ok {
		return incarnation, nil
	}

	return r.reader.ReadAccountIncarnation(address)
}

type ReadLists map[string]*execctx.KvList

func (v ReadLists) Return() {
	returnReadList(v)
}

var readListPool = sync.Pool{
	New: func() any {
		return ReadLists{
			kv.AccountsDomain.String(): {},
			kv.CodeDomain.String():     {},
			kv.StorageDomain.String():  {},
		}
	},
}

func returnReadList(v ReadLists) {
	if v == nil {
		return
	}
	readListPool.Put(v)
}
