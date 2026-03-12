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

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

type StateV3 struct {
	domains *execctx.SharedDomains
	logger  log.Logger
	syncCfg ethconfig.Sync
	txNum   uint64
	trace   bool
}

func NewStateV3(domains *execctx.SharedDomains, syncCfg ethconfig.Sync, logger log.Logger) *StateV3 {
	return &StateV3{
		domains: domains,
		logger:  logger,
		syncCfg: syncCfg,
		//trace: true,
	}
}

func (rs *StateV3) SetTrace(trace bool) {
	rs.trace = trace
}

func (rs *StateV3) Domains() *execctx.SharedDomains {
	return rs.domains
}

func (rs *StateV3) SetTxNum(txNum uint64) {
	rs.txNum = txNum
}

// applyVersionedWrites applies a VersionedWrites slice directly to the shared
// domains without any intermediate BTree representation.
//
// Writes from versionedWriteCollector carry complete account state (all fields
// emitted by UpdateAccountData), so no domain reads are needed to reconstruct
// the full serialised account. SelfDestructPath=true signals either:
//   - pure account deletion (no account fields follow) — from DeleteAccount
//   - code+storage cleanup before recreation — from UpdateAccountData when
//     original.Incarnation > account.Incarnation (followed by account fields)
func (rs *StateV3) applyVersionedWrites(roTx kv.TemporalTx, blockNum, txNum uint64, writes VersionedWrites, balanceIncreases map[accounts.Address]uint256.Int, rules *chain.Rules) error {
	domains := rs.domains

	if len(writes) > 0 {
		type addrState struct {
			balance      *uint256.Int
			nonce        *uint64
			incarnation  *uint64
			codeHash     *accounts.CodeHash
			code         []byte
			selfDestruct bool
			storage      []storageItem
		}

		perAddr := make(map[accounts.Address]*addrState, len(writes)/4+1)
		for _, w := range writes {
			if w.Val == nil {
				continue
			}
			d := perAddr[w.Address]
			if d == nil {
				d = &addrState{}
				perAddr[w.Address] = d
			}
			switch w.Path {
			case BalancePath:
				v := w.Val.(uint256.Int)
				d.balance = &v
			case NoncePath:
				v := w.Val.(uint64)
				d.nonce = &v
			case IncarnationPath:
				v := w.Val.(uint64)
				d.incarnation = &v
			case CodeHashPath:
				v := w.Val.(accounts.CodeHash)
				d.codeHash = &v
			case CodePath:
				d.code = w.Val.([]byte)
			case SelfDestructPath:
				d.selfDestruct = w.Val.(bool)
			case StoragePath:
				d.storage = append(d.storage, storageItem{w.Key, w.Val.(uint256.Int)})
			}
		}

		// Sort addresses before iterating so that trace output is deterministic.
		// Domain writes are buffered into a sorted BTree by key, so order of
		// iteration does not affect correctness — only debug reproducibility.
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
				if dbg.TraceApply && (rs.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d apply:del code+storage: %x\n", blockNum, addr)
				}
				if err := domains.DomainDel(kv.CodeDomain, roTx, address[:], txNum, nil); err != nil {
					return err
				}
				if err := domains.DomainDelPrefix(kv.StorageDomain, roTx, address[:], txNum); err != nil {
					return err
				}
				// Pure delete: no account fields means DeleteAccount was called.
				if d.balance == nil && d.nonce == nil && d.incarnation == nil && d.codeHash == nil {
					if dbg.TraceApply && (rs.trace || dbg.TraceAccount(addr.Handle())) {
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

			if d.balance != nil || d.nonce != nil || d.incarnation != nil || d.codeHash != nil || d.code != nil {
				// versionedWriteCollector.UpdateAccountData always emits all four
				// account fields (balance, nonce, incarnation, codeHash) so the
				// reconstruction below produces a complete account.  The nil-guards
				// remain to defend against partial writes from other code paths.
				acc := accounts.NewAccount()
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
				} else if d.code != nil {
					acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(d.code))
				}
				if dbg.TraceApply && (rs.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d apply:put account: %x balance:%d,nonce:%d,codehash:%x\n", blockNum, addr, &acc.Balance, acc.Nonce, acc.CodeHash)
				}
				if err := domains.DomainPut(kv.AccountsDomain, roTx, address[:], accounts.SerialiseV3(&acc), txNum, nil); err != nil {
					return err
				}
			}

			if d.code != nil {
				if dbg.TraceApply && (rs.trace || dbg.TraceAccount(addr.Handle())) {
					code := d.code
					if len(code) > 40 {
						code = code[:40]
					}
					fmt.Printf("%d apply:put code: %x %x\n", blockNum, addr, code)
				}
				if err := domains.DomainPut(kv.CodeDomain, roTx, address[:], d.code, txNum, nil); err != nil {
					return err
				}
			}

			for _, item := range d.storage {
				key := item.key.Value()
				composite := append(address[:], key[:]...)
				v := item.value.Bytes()
				if len(v) == 0 {
					if dbg.TraceApply && (rs.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d apply:del storage: %x %x\n", blockNum, addr, item.key)
					}
					if err := domains.DomainDel(kv.StorageDomain, roTx, composite, txNum, nil); err != nil {
						return err
					}
				} else {
					if dbg.TraceApply && (rs.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d apply:put storage: %x %x %x\n", blockNum, addr, item.key, &item.value)
					}
					if err := domains.DomainPut(kv.StorageDomain, roTx, composite, v, txNum, nil); err != nil {
						return err
					}
				}
			}
		}
	}

	var acc accounts.Account
	emptyRemoval := rules.IsSpuriousDragon
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
		if emptyRemoval && acc.Nonce == 0 && acc.Balance.IsZero() && acc.IsEmptyCodeHash() {
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

// ApplyStateWrites applies account/storage/code mutations to SharedDomains.
func (rs *StateV3) ApplyStateWrites(ctx context.Context,
	roTx kv.TemporalTx,
	blockNum uint64,
	txNum uint64,
	writes VersionedWrites,
	balanceIncreases map[accounts.Address]uint256.Int,
	rules *chain.Rules,
) error {
	if err := rs.applyVersionedWrites(roTx, blockNum, txNum, writes, balanceIncreases, rules); err != nil {
		return fmt.Errorf("StateV3.ApplyStateWrites: %w", err)
	}
	return nil
}

// ApplyTxIndexes writes trace indices, log indices, and receipts.
func (rs *StateV3) ApplyTxIndexes(
	roTx kv.TemporalTx,
	txNum uint64,
	receipt *types.Receipt,
	cummulativeBlobGas uint64,
	logs []*types.Log,
	traceFroms map[accounts.Address]struct{},
	traceTos map[accounts.Address]struct{},
) error {
	if err := rs.applyLogsAndTraces4(roTx, txNum, receipt, cummulativeBlobGas, logs, traceFroms, traceTos, false); err != nil {
		return fmt.Errorf("StateV3.ApplyTxIndexes: %w", err)
	}
	return nil
}

// CommitStepBoundary computes and persists a trie commitment when txNum falls
// on an aggregation step boundary. This ensures commitment domain snapshots
// contain a commitment state at each step end, even when the boundary falls
// mid-block.
func (rs *StateV3) CommitStepBoundary(ctx context.Context, roTx kv.TemporalTx, blockNum, txNum uint64) error {
	if (txNum+1)%rs.domains.StepSize() == 0 && !dbg.DiscardCommitment() {
		_, err := rs.domains.ComputeCommitment(ctx, roTx, true, blockNum, txNum,
			fmt.Sprintf("applying step %d", txNum/rs.domains.StepSize()), nil)
		if err != nil {
			return fmt.Errorf("StateV3.CommitStepBoundary: %w", err)
		}
	}
	return nil
}

func (rs *StateV3) applyLogsAndTraces4(tx kv.TemporalTx, txNum uint64, receipt *types.Receipt, cummulativeBlobGas uint64, logs []*types.Log, traceFroms map[accounts.Address]struct{}, traceTos map[accounts.Address]struct{}, historyExecution bool) error {
	domains := rs.domains
	for addr := range traceFroms {
		addrValue := addr.Value()
		if err := domains.IndexAdd(kv.TracesFromIdx, addrValue[:], txNum); err != nil {
			return err
		}
	}

	for addr := range traceTos {
		addrValue := addr.Value()
		if err := domains.IndexAdd(kv.TracesToIdx, addrValue[:], txNum); err != nil {
			return err
		}
	}

	for _, lg := range logs {
		if err := domains.IndexAdd(kv.LogAddrIdx, lg.Address[:], txNum); err != nil {
			return err
		}
		for _, topic := range lg.Topics {
			if err := domains.IndexAdd(kv.LogTopicIdx, topic[:], txNum); err != nil {
				return err
			}
		}
	}

	if receipt != nil {
		if !historyExecution {
			blockLogIndex := receipt.FirstLogIndexWithinBlock
			if !rawtemporaldb.ReceiptStoresFirstLogIdx(tx) {
				blockLogIndex += uint32(len(receipt.Logs))
			}
			if err := rawtemporaldb.AppendReceipt(rs.domains.AsPutDel(tx), blockLogIndex, receipt.CumulativeGasUsed, cummulativeBlobGas, txNum); err != nil {
				return err
			}
		}
	}

	if rs.syncCfg.PersistReceiptsCacheV2 {
		if err := rawdb.WriteReceiptCacheV2(rs.domains.AsPutDel(tx), receipt, txNum); err != nil {
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
	sz *= 2 // to cover data-structures overhead: map, btree, etc... and GC overhead (clean happening periodically)
	sz *= 2 // for Commitment calculation when batch is full
	return sz
}

// SizeEstimateAfterCommitment - not including any additional estimations. Use it after ComputeCommitment calc - to see
func (rs *StateV3) SizeEstimateAfterCommitment() uint64 {
	if rs.domains == nil {
		return 0
	}
	sz := rs.domains.Size()
	sz *= 2 // to cover data-structures overhead: map, btree, etc... and GC overhead (clean happening periodically)
	return sz
}

type storageItem struct {
	key   accounts.StorageKey
	value uint256.Int
}

var deleted accounts.Account

type bufferedAccount struct {
	data    *accounts.Account
	code    []byte
	storage *btree.BTreeG[storageItem]
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

func (s *StateV3Buffered) WithDomains(domains *execctx.SharedDomains) *StateV3Buffered {
	return &StateV3Buffered{
		StateV3:       NewStateV3(domains, s.syncCfg, s.logger),
		accounts:      s.accounts,
		accountsMutex: s.accountsMutex,
	}
}

// versionedWriteCollector implements StateWriter and collects writes as a
// flat VersionedWrites slice for direct domain apply via applyVersionedWrites.
//
// When FinalizeTx calls UpdateAccountData, complete account state (all fields)
// is emitted so that applyVersionedWrites can reconstruct the full serialised
// account without domain reads. Code and storage writes are collected similarly.
//
// It also maintains rs.accounts synchronously for the cross-block timing hole
// bridge: block N+1 workers may read block N state before the async applyResults
// goroutine has flushed it to SharedDomains.
type versionedWriteCollector struct {
	rs     *StateV3Buffered
	writes VersionedWrites
}

// NewVersionedWriteCollector creates a versionedWriteCollector that collects
// StateWriter calls into a VersionedWrites slice and maintains rs.accounts.
func NewVersionedWriteCollector(rs *StateV3Buffered) *versionedWriteCollector {
	return &versionedWriteCollector{rs: rs}
}

// Writes returns the collected VersionedWrites for domain apply.
func (c *versionedWriteCollector) Writes() VersionedWrites { return c.writes }

func (c *versionedWriteCollector) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	// Copy to prevent aliasing with pooled stateObjects. After tx finalization
	// the stateObject is returned to the pool and its Account may be overwritten.
	var accountCopy accounts.Account
	accountCopy.Copy(account)
	accountCopy.PrevIncarnation = account.PrevIncarnation

	// When the original incarnation was higher than the new incarnation
	// (pre-existing contract destroyed then recreated at a lower incarnation),
	// emit a SelfDestructPath write first to signal code+storage cleanup before
	// the new account state is written. applyVersionedWrites detects the presence
	// of account fields after SelfDestructPath to distinguish cleanup+recreate
	// from a pure account deletion.
	if original.Incarnation > accountCopy.Incarnation {
		c.writes = append(c.writes, &VersionedWrite{Address: address, Path: SelfDestructPath, Val: true})
	}

	// Emit complete account state as individual VersionedWrites so that
	// applyVersionedWrites can reconstruct the full serialised account.
	c.writes = append(c.writes,
		&VersionedWrite{Address: address, Path: BalancePath, Val: accountCopy.Balance},
		&VersionedWrite{Address: address, Path: NoncePath, Val: accountCopy.Nonce},
		&VersionedWrite{Address: address, Path: IncarnationPath, Val: accountCopy.Incarnation},
		&VersionedWrite{Address: address, Path: CodeHashPath, Val: accountCopy.CodeHash},
	)

	// Maintain rs.accounts for the cross-block timing hole bridge.
	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok || obj.data == &deleted {
		obj = &bufferedAccount{}
	}
	obj.data = &accountCopy
	c.rs.accounts[address] = obj
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	c.writes = append(c.writes, &VersionedWrite{Address: address, Path: CodePath, Val: code})

	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok || obj.data == &deleted {
		obj = &bufferedAccount{}
		c.rs.accounts[address] = obj
	}
	obj.code = code
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	c.writes = append(c.writes, &VersionedWrite{Address: address, Path: SelfDestructPath, Val: true})

	c.rs.accountsMutex.Lock()
	obj, ok := c.rs.accounts[address]
	if !ok {
		obj = &bufferedAccount{data: &deleted}
		c.rs.accounts[address] = obj
	}
	*obj = bufferedAccount{data: &deleted}
	c.rs.accountsMutex.Unlock()

	return nil
}

func (c *versionedWriteCollector) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	if original == value {
		return nil
	}

	c.writes = append(c.writes, &VersionedWrite{Address: address, Path: StoragePath, Key: key, Val: value})

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
func NotifyAccumulator(accumulator *shards.Accumulator, writes VersionedWrites) {
	if accumulator == nil || len(writes) == 0 {
		return
	}

	type pendingAccount struct {
		balance     *uint256.Int
		nonce       *uint64
		incarnation *uint64
		codeHash    *accounts.CodeHash
	}

	pending := make(map[accounts.Address]*pendingAccount, len(writes)/4+1)

	for _, w := range writes {
		if w.Val == nil {
			continue
		}
		switch w.Path {
		case BalancePath:
			p := pending[w.Address]
			if p == nil {
				p = &pendingAccount{}
				pending[w.Address] = p
			}
			v := w.Val.(uint256.Int)
			p.balance = &v
		case NoncePath:
			p := pending[w.Address]
			if p == nil {
				p = &pendingAccount{}
				pending[w.Address] = p
			}
			v := w.Val.(uint64)
			p.nonce = &v
		case IncarnationPath:
			p := pending[w.Address]
			if p == nil {
				p = &pendingAccount{}
				pending[w.Address] = p
			}
			v := w.Val.(uint64)
			p.incarnation = &v
		case CodeHashPath:
			p := pending[w.Address]
			if p == nil {
				p = &pendingAccount{}
				pending[w.Address] = p
			}
			v := w.Val.(accounts.CodeHash)
			p.codeHash = &v
		case CodePath:
			code := w.Val.([]byte)
			var inc uint64
			if p := pending[w.Address]; p != nil && p.incarnation != nil {
				inc = *p.incarnation
			}
			accumulator.ChangeCode(w.Address.Value(), inc, code)
		case StoragePath:
			val := w.Val.(uint256.Int)
			var inc uint64
			if p := pending[w.Address]; p != nil && p.incarnation != nil {
				inc = *p.incarnation
			}
			accumulator.ChangeStorage(w.Address.Value(), inc, w.Key.Value(), val.Bytes())
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
		//trace: true,
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
	if err := w.tx.DomainPut(kv.CodeDomain, addressValue[:], code, w.txNum, nil); err != nil {
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
	//TODO: move logic from SD
	//if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
	//	return err
	//}
	//if err := w.tx.DomainDel(kv.CodeDomain, address[:], nil); err != nil {
	//	return err
	//}
	addressValue := address.Value()
	if err := w.tx.DomainDel(kv.AccountsDomain, addressValue[:], w.txNum, nil); err != nil {
		return err
	}
	// if w.accumulator != nil { TODO: investigate later. basically this will always panic. keeping this out should be fine anyway.
	// 	w.accumulator.DeleteAccount(address)
	// }
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
	composite := append(addressValue[:], keyValue[:]...)
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

type ReaderV3 struct {
	txNum       uint64
	trace       bool
	tracePrefix string
	getter      kv.TemporalGetter
}

func NewReaderV3(getter kv.TemporalGetter) *ReaderV3 {
	return &ReaderV3{
		//trace:  true,
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

func (r *ReaderV3) HasStorage(address accounts.Address) (bool, error) {
	var value common.Address
	if !address.IsNil() {
		value = address.Value()
	}
	// this is an optimization, but also checks the account is checked in the domain
	// for being deleted on unwind before we try to access the storage
	if enc, _, err := r.getter.GetLatest(kv.AccountsDomain, value[:]); len(enc) == 0 {
		return false, err
	}
	_, _, hasStorage, err := r.getter.HasPrefix(kv.StorageDomain, value[:])
	return hasStorage, err
}

func (r *ReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	_, acc, err := r.readAccountData(address)
	return acc, err
}

func (r *ReaderV3) readAccountData(address accounts.Address) ([]byte, *accounts.Account, error) {
	var value common.Address
	if !address.IsNil() {
		value = address.Value()
	}
	enc, _, err := r.getter.GetLatest(kv.AccountsDomain, value[:])
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
	var composite [20 + 32]byte
	var addressValue common.Address
	if !address.IsNil() {
		addressValue = address.Value()
	}
	var keyValue common.Hash
	if !key.IsNil() {
		keyValue = key.Value()
	}
	copy(composite[0:20], addressValue[0:20])
	copy(composite[20:], keyValue[:])
	enc, _, err := r.getter.GetLatest(kv.StorageDomain, composite[:])
	if err != nil {
		return uint256.Int{}, false, err
	}

	ok := enc != nil
	var res uint256.Int
	if ok {
		(&res).SetBytes(enc)
	}

	if r.trace {
		if enc == nil {
			fmt.Printf("%sReadAccountStorage [%x %x] => [empty], txNum: %d, stack: %s\n", r.tracePrefix, address, key, r.txNum, dbg.Stack())
		} else {
			fmt.Printf("%sReadAccountStorage [%x %x] => [%x], txNum: %d, stack: %s\n", r.tracePrefix, address, key, &res, r.txNum, dbg.Stack())
		}
	}

	return res, ok, err
}

func (r *ReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	var addressValue common.Address
	if !address.IsNil() {
		addressValue = address.Value()
	}
	enc, _, err := r.getter.GetLatest(kv.CodeDomain, addressValue[:])
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
	var addressValue common.Address
	if !address.IsNil() {
		addressValue = address.Value()
	}
	enc, _, err := r.getter.GetLatest(kv.CodeDomain, addressValue[:])
	if err != nil {
		return 0, err
	}
	size := len(enc)
	if r.trace {
		fmt.Printf("%sReadAccountCodeSize [%x] => [%d], txNum: %d\n", r.tracePrefix, addressValue, size, r.txNum)
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
			// TODO - we really need to return the first key
			// for this we need to order the list of hashes
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
	//for _, tbl := range v {
	//	clear(tbl.Keys)
	//	clear(tbl.Vals)
	//	tbl.Keys, tbl.Vals = tbl.Keys[:0], tbl.Vals[:0]
	//}
	readListPool.Put(v)
}
