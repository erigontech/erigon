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
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
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

	// Scratch for the per-transaction index writes; the apply goroutine alone
	// touches them.
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

// ApplyWrites writes an already-Normalized write-set directly to the shared
// domains — the single write-set commit path shared by the parallel executor and
// block production. Writes carry complete account state, so no domain reads are
// needed to reconstruct the serialised account.
func ApplyWrites(writes WriteSetView, domains *execctx.SharedDomains, roTx kv.TemporalTx, blockNum, txNum uint64, balanceIncreases map[accounts.Address]uint256.Int, rules *chain.Rules, trace bool) error {
	if writes != nil && !writes.IsEmpty() {
		type addrState struct {
			balance        *uint256.Int
			nonce          *uint64
			incarnation    *uint64
			codeHash       *accounts.CodeHash
			code           []byte
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
			v := vw.Val
			ensure(a).balance = &v
		}
		for a, vw := range writes.Nonces() {
			v := vw.Val
			ensure(a).nonce = &v
		}
		for a, vw := range writes.Incarnations() {
			v := vw.Val
			ensure(a).incarnation = &v
		}
		// CodeHashes before Codes: an explicit CodeHashPath write wins; a code
		// write only supplies the hash when no explicit one was recorded.
		for a, vw := range writes.CodeHashes() {
			v := vw.Val
			ensure(a).codeHash = &v
		}
		for a, vw := range writes.Codes() {
			d := ensure(a)
			d.code = vw.Val.Bytes
			d.codeWritten = true
		}
		for a, vw := range writes.SelfDestructs() {
			ensure(a).selfDestruct = vw.Val
		}
		for a, vw := range writes.CreateContracts() {
			ensure(a).createContract = vw.Val
		}
		for a, byKey := range writes.Storages() {
			d := ensure(a)
			for k, vw := range byKey {
				d.storage = append(d.storage, storageItem{k, vw.Val})
			}
		}

		// Drop a self-destructed address's own field/storage writes so it reaches
		// the pure-delete branch below (a raw versionMap view still carries them).
		// Balance is kept: its zero-ness drives the EIP-8246 preserved-balance vs
		// pure-delete decision.
		for _, d := range perAddr {
			if d.selfDestruct {
				d.nonce = nil
				d.incarnation = nil
				d.codeHash = nil
				d.code = nil
				d.codeWritten = false
				d.storage = nil
			}
		}

		// Sort addresses only for deterministic trace output; domain writes are
		// buffered into a sorted BTree, so iteration order is not load-bearing.
		addrs := make([]accounts.Address, 0, len(perAddr))
		for addr := range perAddr {
			addrs = append(addrs, addr)
		}
		slices.SortFunc(addrs, func(a, b accounts.Address) int {
			av, bv := a.Value(), b.Value()
			return bytes.Compare(av[:], bv[:])
		})

		getLatestAcct := func(k []byte) ([]byte, error) {
			v, _, err := domains.GetLatest(kv.AccountsDomain, roTx, k)
			return v, err
		}

		for _, addr := range addrs {
			d := perAddr[addr]
			address := addr.Value()

			if d.selfDestruct {
				if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d apply:del code+storage: %x\n", blockNum, addr)
				}
				// An EIP-8246 self-destruct keeps its balance write; the record
				// survives only when a non-zero balance is left to preserve.
				sdPreservedBalance := d.balance != nil && !d.balance.IsZero()
				pureDelete := !sdPreservedBalance && d.nonce == nil && d.incarnation == nil && d.codeHash == nil
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
				// Otherwise: cleanup code+storage before recreating account
				// (originalIncarnation > account.Incarnation case).
			}

			// Contract creation: clear stale storage before writing new account.
			if d.createContract {
				hasAcc, err := hasCommittedAccount(domains, roTx, address[:])
				if err != nil {
					return err
				}
				if !hasAcc {
					if err := assertNoCommittedStorage(domains, roTx, address[:], "createContract"); err != nil {
						return err
					}
				} else if err := domains.DomainDelPrefix(kv.StorageDomain, roTx, address[:], txNum); err != nil {
					return err
				}
			}

			if d.balance != nil || d.nonce != nil || d.incarnation != nil || d.codeHash != nil || d.codeWritten {
				// A WriteSet may contain only the changed account fields, so
				// overlay it on the current state. Self-destruct is the exception:
				// its base stays empty so cleared fields cannot be resurrected.
				acc := accounts.NewAccount()
				if !d.selfDestruct {
					if vw, ok := writes.(*versionMapWriteView); ok {
						// Compose the pre-tx account from the versionMap (seeded origin
						// plus prior txs' field cells); every account written this block
						// has a seeded origin, so the compose is complete.
						if base := NewVersionedAccountView(addr, vw.txIdx, vw.vm, nil).Account(); base != nil {
							acc = *base
						}
					} else if enc0, err := getLatestAcct(address[:]); err == nil && len(enc0) > 0 {
						_ = accounts.DeserialiseV3(&acc, enc0)
					}
				}
				if d.balance != nil {
					acc.Balance = *d.balance
				}
				if d.nonce != nil {
					acc.Nonce = *d.nonce
				}
				if d.incarnation != nil {
					acc.Incarnation = *d.incarnation
				}
				if d.codeHash != nil {
					acc.CodeHash = *d.codeHash
				} else if d.codeWritten {
					acc.CodeHash = accounts.NewCode(d.code).Hash
				}
				if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d apply:put account: %x balance:%s,nonce:%d,codehash:%x\n", blockNum, addr, acc.Balance.String(), acc.Nonce, acc.CodeHash)
				}
				enc := accounts.SerialiseV3(&acc)
				if err := domains.DomainPut(kv.AccountsDomain, roTx, address[:], enc, txNum, nil); err != nil {
					return err
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
				if len(d.code) == 0 {
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
					if err := domains.DomainDel(kv.StorageDomain, roTx, composite, txNum, nil); err != nil {
						return err
					}
				} else {
					if dbg.TraceApply && (trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d apply:put storage: %x %x %x\n", blockNum, addr, item.key, v)
					}
					if err := domains.DomainPut(kv.StorageDomain, roTx, composite, v, txNum, nil); err != nil {
						return err
					}
				}
			}
		}
	}

	var acc accounts.Account
	for addr, increase := range balanceIncreases {
		addrValue := addr.Value()
		enc0, _, err := domains.GetLatest(kv.AccountsDomain, roTx, addrValue[:])
		if err != nil {
			return err
		}
		acc.Reset()
		if len(enc0) > 0 {
			if err := accounts.DeserialiseV3(&acc, enc0); err != nil {
				return err
			}
		}
		acc.Balance.Add(&acc.Balance, &increase)
		// Incarnation==0 guard: see the account-write EIP-161 site above.
		if EIP161EmptyRemoval(rules.IsEIP161Enabled(), rules.IsAura, addr) && acc.Nonce == 0 && acc.Balance.IsZero() && acc.IsEmptyCodeHash() && acc.Incarnation == 0 {
			if err := domains.DomainDel(kv.AccountsDomain, roTx, addrValue[:], txNum, enc0); err != nil {
				return err
			}
		} else {
			enc1 := accounts.SerialiseV3(&acc)
			if err := domains.DomainPut(kv.AccountsDomain, roTx, addrValue[:], enc1, txNum, enc0); err != nil {
				return err
			}
		}
	}
	return nil
}

// ApplyStateWrites applies account/storage/code mutations directly to
// SharedDomains via DomainPut at the given per-tx txNum.
func (rs *StateV3) ApplyStateWrites(_ context.Context,
	roTx kv.TemporalTx,
	blockNum uint64,
	txNum uint64,
	writes WriteSetView,
	balanceIncreases map[accounts.Address]uint256.Int,
	rules *chain.Rules,
) error {
	if (writes == nil || writes.IsEmpty()) && len(balanceIncreases) == 0 {
		return nil
	}
	if err := ApplyWrites(writes, rs.domains, roTx, blockNum, txNum, balanceIncreases, rules, rs.trace.Load()); err != nil {
		return fmt.Errorf("StateV3.ApplyStateWrites: %w", err)
	}
	return nil
}

// ApplyTxIndexes writes trace indices, log indices, and receipts. skipReceiptCache
// skips the receipt-cache domain write for the parallel executor's block-finalize
// txResult, which shares the system-tx-end txNum: a second DomainPut there would
// overwrite the last regular tx's receipt history entry.
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

// CommitStepBoundary computes and persists a trie commitment when txNum falls
// on an aggregation step boundary. This ensures commitment domain snapshots
// contain a commitment state at each step end, even when the boundary falls
// mid-block.
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

// SizeEstimateBeforeCommitment - including esitmation of future ComputeCommitment on current state changes
func (rs *StateV3) SizeEstimateBeforeCommitment() uint64 {
	if rs.domains == nil {
		return 0
	}
	sz := rs.domains.Size()
	sz *= 2 // for Commitment calculation when batch is full
	return sz
}

// SizeEstimateAfterCommitment - not including any additional estimations. Use it after ComputeCommitment calc - to see
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

// ClearAccountsCache drops all entries from the cross-block account cache. Must
// be called after a block's writes are fully applied to SharedDomains and before
// the next block's workers read, or stale entries leak forward and workers read
// outdated values from the cache instead of sd.mem.
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

// versionedWriteCollector implements StateWriter, collecting writes as a WriteSet
// for direct domain apply. It emits complete account state so apply needs no
// domain reads, and maintains rs.accounts synchronously to bridge the cross-block
// timing hole: block N+1 workers may read block N state before the async apply
// goroutine has flushed it to SharedDomains.
type versionedWriteCollector struct {
	rs     *StateV3Buffered
	writes *WriteSet
}

// NewVersionedWriteCollector creates a versionedWriteCollector that collects
// StateWriter calls into a VersionedWrites slice and maintains rs.accounts.
func NewVersionedWriteCollector(rs *StateV3Buffered) *versionedWriteCollector {
	return &versionedWriteCollector{rs: rs, writes: &WriteSet{}}
}

// Writes returns the collected write set for domain apply.
func (c *versionedWriteCollector) Writes() *WriteSet { return c.writes }

func (c *versionedWriteCollector) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	// Copy to prevent aliasing with pooled stateObjects. After tx finalization
	// the stateObject is returned to the pool and its Account may be overwritten.
	var accountCopy accounts.Account
	accountCopy.Copy(account)
	accountCopy.PrevIncarnation = account.PrevIncarnation

	// A down-rev incarnation (contract destroyed then recreated) emits a
	// SelfDestructPath write first to signal code+storage cleanup before the new
	// account state; apply distinguishes cleanup+recreate from a pure delete by
	// the account fields that follow.
	needsCleanup := original.Incarnation > accountCopy.Incarnation
	// Cross-block reincarnation: a prior-block self-destruct reads back as a nil
	// account (Incarnation=0), so the check above misses it; rs.accounts still
	// holds the deleted marker since blocks run sequentially.
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
	// IBS records any accompanying incarnation change separately.
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

// NotifyAccumulator drives txpool state-diff notifications from VersionedWrites.
// It reconstructs account state from the per-field writes and calls
// ChangeAccount/ChangeCode/ChangeStorage on the accumulator. StartChange must
// have been called on the accumulator before this function is invoked.
func NotifyAccumulator(accumulator *shards.Accumulator, writes WriteSetView) {
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

	// Flush pending account field groups.
	for addr, p := range pending {
		if p.balance == nil && p.nonce == nil {
			continue // no account fields collected (e.g. only storage/code)
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

// Writer - used by parallel workers to accumulate updates and then send them to conflict-resolution.
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

// hasCommittedAccount probes the account domain for addr. An address with no
// committed account holds no committed storage (storage is only written for an
// existing account, and deleting one wipes its storage prefix), so this cheap
// existence probe stands in for a full storage-prefix walk.
func hasCommittedAccount(domains *execctx.SharedDomains, roTx kv.TemporalTx, addr []byte) (bool, error) {
	enc, _, err := domains.GetLatest(kv.AccountsDomain, roTx, addr)
	if err != nil {
		return false, err
	}
	return len(enc) > 0, nil
}

// assertNoCommittedStorage panics when addr has committed storage but no
// committed account, surfacing a violation of the hasCommittedAccount invariant.
// No-op unless asserts are enabled.
func assertNoCommittedStorage(domains *execctx.SharedDomains, roTx kv.TemporalTx, addr []byte, what string) error {
	if !dbg.AssertEnabled {
		return nil
	}
	// IteratePrefix does not resolve through sd.parent (the account probe does),
	// so re-read each hit the way the probe reads or a parent-deleted address
	// trips the assert. The re-reads must follow the walk: IteratePrefix holds the
	// domain RLock across its callback and GetLatest retakes it, deadlocking
	// against a writer queued between them.
	var candidates [][]byte
	if err := domains.IteratePrefix(kv.StorageDomain, addr, roTx, func(k, v []byte) (bool, error) {
		candidates = append(candidates, bytes.Clone(k))
		return true, nil
	}); err != nil {
		return err
	}
	for _, k := range candidates {
		cur, _, err := domains.GetLatest(kv.StorageDomain, roTx, k)
		if err != nil {
			return err
		}
		if len(cur) > 0 {
			panic(fmt.Sprintf("%s: %x has storage but no account", what, addr))
		}
	}
	return nil
}

func (w *Writer) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("Writer: acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	addressValue := address.Value()
	if original.Incarnation > account.Incarnation {
		//del, before create: to clanup code/storage
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

func (w *Writer) CreateContract(address accounts.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
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
	getter      execctxapi.StateGetter

	// Reused key buffers: as fields of the heap-allocated reader, the key slices
	// they back reach the interface getter with no per-call heap allocation.
	addr      common.Address                  // account/code lookup key
	composite [length.Addr + length.Hash]byte // storage lookup key (addr||slot)
}

func NewReaderV3(getter execctxapi.StateGetter) *ReaderV3 {
	return &ReaderV3{
		getter: getter,
	}
}

func (r *ReaderV3) DiscardReadList()                        {}
func (r *ReaderV3) SetTxNum(txNum uint64)                   { r.txNum = txNum }
func (r *ReaderV3) SetGetter(getter execctxapi.StateGetter) { r.getter = getter }

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

func (r *ReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	_, acc, err := r.readAccountData(address)
	return acc, err
}

func (r *ReaderV3) readAccountData(address accounts.Address) ([]byte, *accounts.Account, error) {
	r.addr = address.Value()
	enc, _, err := r.getter.GetLatest(kv.AccountsDomain, r.addr[:], kv.GetLatestOptions{})
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
	enc, _, err := r.getter.GetLatest(kv.StorageDomain, r.composite[:], kv.GetLatestOptions{})
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
	enc, _, err := r.getter.GetCode(r.addr[:], r.txNum)
	if err != nil {
		return nil, err
	}
	if r.trace {
		lenc, cs := printCode(enc)
		fmt.Printf("%sReadAccountCode [%x] =>  [%d:%s], txNum: %d, stack: %s\n", r.tracePrefix, address, lenc, cs, r.txNum, dbg.Stack())
	}
	return enc, nil
}

func (r *ReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	r.addr = address.Value()
	size, _, err := r.getter.GetCodeSize(r.addr[:], r.txNum)
	if err != nil {
		return 0, err
	}
	if r.trace {
		fmt.Printf("%sReadAccountCodeSize (sz) [%x] => [%d], txNum: %d\n", r.tracePrefix, r.addr, size, r.txNum)
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

func (r *latestBufferedReader) SetGetter(getter execctxapi.StateGetter) {
	r.reader.(interface{ SetGetter(execctxapi.StateGetter) }).SetGetter(getter)
}

type historicBufferedReader struct {
	bufferedReader
}

func (r *historicBufferedReader) SetTx(tx kv.TemporalTx) {
	r.reader.(interface{ SetTx(kv.TemporalTx) }).SetTx(tx)
}

func NewBufferedReader(bufferedState *StateV3Buffered, reader StateReader) StateReader {
	type latest interface {
		SetGetter(execctxapi.StateGetter)
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
	// Not optional: Vals pins what the txn read until the list is reused.
	for _, tbl := range v {
		clear(tbl.Keys)
		clear(tbl.Vals)
		tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	}
	readListPool.Put(v)
}

type LightCollector struct {
	writes *WriteSet
}

func NewLightCollector() *LightCollector {
	return &LightCollector{writes: &WriteSet{}}
}

// TakeWrites returns the accumulated writes and resets the collector.
func (c *LightCollector) TakeWrites() *WriteSet {
	writes := c.writes
	c.writes = &WriteSet{}
	return writes
}

func (c *LightCollector) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	var accountCopy accounts.Account
	accountCopy.Copy(account)
	accountCopy.PrevIncarnation = account.PrevIncarnation

	if original.Incarnation > accountCopy.Incarnation {
		c.writes.SetSelfDestruct(address, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: address, Path: SelfDestructPath}, Val: true})
	}

	// Only emit fields that changed vs original. original is the worker's
	// block-origin snapshot, so emitting an unchanged field would overwrite a
	// later tx's update on apply with a stale pre-block value.
	if !accountCopy.Balance.Eq(&original.Balance) {
		c.writes.SetBalance(address, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: address, Path: BalancePath}, Val: accountCopy.Balance})
	}
	if accountCopy.Nonce != original.Nonce {
		c.writes.SetNonce(address, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: address, Path: NoncePath}, Val: accountCopy.Nonce})
	}
	// Emit on up-revs only — a down-rev would clobber a same-block SD-side cell.
	if accountCopy.Incarnation > original.Incarnation {
		c.writes.SetIncarnation(address, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: address, Path: IncarnationPath}, Val: accountCopy.Incarnation})
	}
	if accountCopy.CodeHash != original.CodeHash {
		c.writes.SetCodeHash(address, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: address, Path: CodeHashPath}, Val: accountCopy.CodeHash})
	}
	return nil
}

func (c *LightCollector) UpdateAccountCode(address accounts.Address, _ uint64, codeHash accounts.CodeHash, code []byte) error {
	c.writes.SetCode(address, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: address, Path: CodePath}, Val: accounts.Code{Hash: codeHash, Bytes: code}})
	return nil
}

func (c *LightCollector) DeleteAccount(address accounts.Address, _ *accounts.Account) error {
	c.writes.SetSelfDestruct(address, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: address, Path: SelfDestructPath}, Val: true})
	return nil
}

func (c *LightCollector) WriteAccountStorage(address accounts.Address, _ uint64, key accounts.StorageKey, _, value uint256.Int) error {
	c.writes.SetStorage(address, key, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: address, Path: StoragePath, Key: key}, Val: value})
	return nil
}

func (c *LightCollector) CreateContract(_ accounts.Address) error { return nil }
