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

package stagedsync

import (
	"context"
	"github.com/erigontech/erigon/execution/types"
	"golang.org/x/sync/errgroup"
	"maps"
	"slices"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The commitment path folds a block's updates straight from the version map instead of
// accumulating per-field values. accumulateTouched collects the block's key-only touched
// union (account keys, storage slots, and the self-destruct subtree-reset set);
// emitTouched then reads each final value from the version map view via
// GetAccount/GetStorage and writes one Update per key. The version map already carries
// the EIP-161 / self-destruct existence verdict, so this path has no EIP knowledge and
// no re-derivation.

func (cc *commitmentCalculator) clearTouched() {
	clear(cc.touchedAccounts)
	clear(cc.touchedStorage)
	clear(cc.destructed)
	cc.baseErr = nil
}

// accumulateTouched folds one write-set view's changed-key set into the touched union.
// An account is marked touched by any real account-field change
// and by any real storage change (so its storageRoot refolds); a slot is
// marked only when it actually changed; a true SelfDestruct marks the subtree reset. A
// bare touch that changes nothing — a net-zero balance write to an absent account — folds
// nothing, exactly as the lifecycle/domain read and the canonical trie treat it.
func (cc *commitmentCalculator) accumulateTouched(writes state.WriteSetView, base state.StateReader) {
	if cc.touchedAccounts == nil {
		cc.touchedAccounts = map[accounts.Address][]byte{}
		cc.touchedStorage = map[accounts.Address]map[accounts.StorageKey][]byte{}
		cc.destructed = map[accounts.Address]struct{}{}
	}
	for a := range writes.AccountChanges() {
		cc.addTouchedAccount(a, base)
	}
	for a, vw := range writes.SelfDestructs() {
		if vw.Val {
			cc.destructed[a] = struct{}{}
		}
	}
	for a, slots := range writes.StorageChanges() {
		cc.addTouchedAccount(a, base)
		m := cc.touchedStorage[a]
		if m == nil {
			m = map[accounts.StorageKey][]byte{}
			cc.touchedStorage[a] = m
		}
		for k := range slots {
			if _, seen := m[k]; seen {
				continue
			}
			address, slot := a.Value(), k.Value()
			plainKey := slices.Concat(address[:], slot[:])
			m[k] = cc.touchHash(plainKey)
			if cc.prefetchFn != nil {
				cc.prefetchFn(plainKey)
			}
		}
	}
}

func (cc *commitmentCalculator) touchHash(plainKey []byte) []byte {
	if cc.hashAtFold {
		return nil
	}
	return cc.updates.HashKey(plainKey)
}

func (cc *commitmentCalculator) addTouchedAccount(a accounts.Address, base state.StateReader) {
	if _, seen := cc.touchedAccounts[a]; seen {
		return
	}
	address := a.Value()
	cc.touchedAccounts[a] = cc.touchHash(address[:])
	if _, prefetched := cc.baseAccounts[a]; base != nil && !prefetched {
		if acc, err := base.ReadAccountData(a); err == nil {
			cc.baseAccounts[a] = acc
		} else if cc.baseErr == nil {
			cc.baseErr = err
		}
	}
	if cc.prefetchFn != nil {
		cc.prefetchFn(address[:])
	}
}

// emitTouched writes one commitment Update per touched key, reading each final value
// from the version map view (pinned at the fold txNum, cascading current map →
// predecessor maps → shared domain): an account absent in the view removes its leaf
// (and subtree), a present one writes its full account; a storage slot cleared to zero
// removes its leaf, a non-zero one writes it.
func (cc *commitmentCalculator) emitTouched(view *state.VersionMapView, updates *commitment.Updates) error {
	if cc.baseErr != nil {
		return cc.baseErr
	}
	for addr, hashed := range cc.touchedAccounts {
		address := addr.Value()
		var u commitment.Update
		acc, ok := view.GetAccount(addr)
		if ok {
			u = commitment.Update{
				Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
				Balance:  acc.Balance,
				Nonce:    acc.Nonce,
				CodeHash: acc.CodeHash.Value(),
			}
			if acc.CodeHash.IsEmpty() {
				u.CodeHash = empty.CodeHash
			}
		} else {
			u = commitment.Update{Flags: commitment.DeleteUpdate, CodeHash: empty.CodeHash, DeleteStorageSubtree: true}
		}
		if _, ok := cc.destructed[addr]; ok || view.WasDestroyed(addr) {
			u.DeleteStorageSubtree = true
		}
		updates.TouchPlainKeyDirectHashed(string(address[:]), hashed, &u)
	}
	for addr, slots := range cc.touchedStorage {
		address := addr.Value()
		for slot, hashed := range slots {
			slotVal := slot.Value()
			var u commitment.Update
			val, ok := view.GetStorage(addr, slot)
			if ok && !val.IsZero() {
				vBytes := val.Bytes()
				u.Flags = commitment.StorageUpdate
				u.StorageLen = int8(len(vBytes))
				copy(u.Storage[:], vBytes)
			} else {
				u.Flags = commitment.DeleteUpdate
			}
			updates.TouchPlainKeyDirectHashed(string(address[:])+string(slotVal[:]), hashed, &u)
		}
	}
	return view.Err()
}

func (cc *commitmentCalculator) setBlockBase(vm *state.VersionMap, base state.StateReader) {
	cc.blockBaseVM, cc.blockBase = vm, base
	if cc.baseAccounts == nil {
		cc.baseAccounts = map[accounts.Address]*accounts.Account{}
	}
	clear(cc.baseAccounts)
}

func (cc *commitmentCalculator) foldView(vm *state.VersionMap, lastTxNum uint64, base state.StateReader) *state.VersionMapView {
	if len(cc.baseAccounts) > 0 && vm == cc.blockBaseVM {
		base = &cachedAccountReader{StateReader: base, accounts: cc.baseAccounts}
	}
	return vm.View(lastTxNum, base)
}

type cachedAccountReader struct {
	state.StateReader
	accounts map[accounts.Address]*accounts.Account
}

func (r *cachedAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if acc, ok := r.accounts[addr]; ok {
		return acc, nil
	}
	return r.StateReader.ReadAccountData(addr)
}

// baseAccountReaders is how many read txs the BAL prefetch of block-start accounts uses.
const baseAccountReaders = 8

// baseAccountPrefetchMinTxs is the block size from which the touch bookkeeping falls behind exec without the prefetch.
const baseAccountPrefetchMinTxs = 1000

// prefetchBaseAccounts reads, in parallel, the block-start values of the accounts a BAL block declares, so the
// per-tx touch bookkeeping does not read them one by one on the calculator's goroutine.
func (cc *commitmentCalculator) prefetchBaseAccounts(ctx context.Context, vm *state.VersionMap, txNum uint64) {
	bal := vm.BALAccounts()
	if len(bal) == 0 || vm.TxCount() < baseAccountPrefetchMinTxs || cc.db == nil || cc.asOfReader == nil {
		return
	}
	got, err := readBaseAccounts(ctx, bal, baseAccountReaders, func() (state.StateReader, func(), error) {
		tx, err := cc.db.BeginTemporalRo(ctx)
		if err != nil {
			return nil, nil, err
		}
		return cc.asOfReader.dr.CloneWithTx(tx).StateReaderAt(txNum), tx.Rollback, nil
	})
	if err != nil {
		return // the touch reads the account itself
	}
	maps.Copy(cc.baseAccounts, got)
}

// readBaseAccounts reads every BAL account through workers readers, each from newReader; an absent account maps to nil.
func readBaseAccounts(ctx context.Context, bal types.BlockAccessList, workers int, newReader func() (state.StateReader, func(), error)) (map[accounts.Address]*accounts.Account, error) {
	parts := make([]map[accounts.Address]*accounts.Account, workers)
	g, ctx := errgroup.WithContext(ctx)
	for w := range workers {
		g.Go(func() error {
			r, done, err := newReader()
			if err != nil {
				return err
			}
			defer done()
			m := make(map[accounts.Address]*accounts.Account, len(bal)/workers+1)
			for i := w; i < len(bal); i += workers {
				if err := ctx.Err(); err != nil {
					return err
				}
				a := accounts.InternAddress(bal[i].Address)
				acc, err := r.ReadAccountData(a)
				if err != nil {
					return err
				}
				m[a] = acc
			}
			parts[w] = m
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	all := make(map[accounts.Address]*accounts.Account, len(bal))
	for _, m := range parts {
		maps.Copy(all, m)
	}
	return all, nil
}
