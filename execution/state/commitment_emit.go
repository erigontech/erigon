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
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// AccountUpdate composes addr's commitment Update as-of the view: a present account
// becomes a full balance/nonce/code update, an absent one a leaf delete. The returned
// bool is the self-destruct / EIP-161 removal verdict (DestroyedAt), which a caller
// tracking cross-block wipes uses to prune the storage subtree.
func (view *VersionMapView) AccountUpdate(addr accounts.Address) (commitment.Update, bool) {
	u := commitment.Update{Flags: commitment.DeleteUpdate, CodeHash: empty.CodeHash, DeleteStorageSubtree: true}
	acc, ok, wasDeleted := view.GetAccount(addr)
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
	}
	return u, wasDeleted
}

// StorageUpdate composes a slot's commitment Update as-of the view: a non-zero value
// writes the leaf, a zero or absent one removes it.
func (view *VersionMapView) StorageUpdate(addr accounts.Address, key accounts.StorageKey) commitment.Update {
	var u commitment.Update
	if val, ok := view.GetStorage(addr, key); ok && !val.IsZero() {
		vBytes := val.Bytes()
		u.Flags = commitment.StorageUpdate
		u.StorageLen = int8(len(vBytes))
		copy(u.Storage[:], vBytes)
	} else {
		u.Flags = commitment.DeleteUpdate
	}
	return u
}

// EmitTouchedAsOf feeds every key the write set touched into updates, reading each
// key's final value as-of view — the read-as-of path the block committer uses, so a
// trie driven from these updates computes the identical root.
func EmitTouchedAsOf(ws WriteSetView, view *VersionMapView, updates *commitment.Updates) error {
	seen := map[accounts.Address]struct{}{}
	for addr := range accountChangeAddrs(ws) {
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}
		address := addr.Value()
		u, _ := view.AccountUpdate(addr)
		updates.TouchPlainKeyDirect(string(address[:]), &u)
	}
	composite := make([]byte, length.Addr+length.Hash)
	for addr, slots := range ws.StorageChanges() {
		address := addr.Value()
		copy(composite, address[:])
		for key := range slots {
			slot := key.Value()
			copy(composite[length.Addr:], slot[:])
			u := view.StorageUpdate(addr, key)
			updates.TouchPlainKeyDirect(string(composite), &u)
		}
	}
	return view.Err()
}
