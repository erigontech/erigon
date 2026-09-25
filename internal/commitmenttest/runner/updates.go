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

package runner

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	keccak "github.com/erigontech/fastkeccak"
)

func Update(op commitmenttest.Op) *commitment.Update {
	if op.Read {
		return nil
	}
	u := &commitment.Update{}
	if op.Delete {
		u.Flags = commitment.DeleteUpdate
		return u
	}
	if op.Account != nil {
		value := op.Account
		u.Nonce, u.Balance, u.CodeHash = value.Nonce, value.Balance, value.CodeHash
		if value.Fields&commitmenttest.NonceField != 0 {
			u.Flags |= commitment.NonceUpdate
		}
		if value.Fields&commitmenttest.BalanceField != 0 {
			u.Flags |= commitment.BalanceUpdate
		}
		if value.Fields&commitmenttest.CodeField != 0 {
			u.Flags |= commitment.CodeUpdate
		}
	} else {
		if len(op.Storage) > len(u.Storage) {
			panic("storage value exceeds 32 bytes")
		}
		u.Flags = commitment.StorageUpdate
		u.StorageLen = int8(len(op.Storage))
		copy(u.Storage[:], op.Storage)
	}
	return u
}

func updatesFor(tb testing.TB, mode commitment.Mode, ops []commitmenttest.Op) *commitment.Updates {
	tb.Helper()
	updates := commitment.NewUpdates(mode, tb.TempDir(), commitment.KeyToHexNibbleHash)
	tb.Cleanup(updates.Close)
	for _, op := range ops {
		if op.Read {
			updates.TouchPlainKey(string(op.Key), nil, func(*commitment.KeyUpdate, []byte) {})
			continue
		}
		updates.TouchPlainKeyDirect(string(op.Key), Update(op))
	}
	return updates
}

func Feed(ops []commitmenttest.Op) (*commitment.Feed, error) {
	feed := &commitment.Feed{Keys: len(ops)}
	index := make(map[string]int)
	for _, op := range ops {
		if op.Read || (len(op.Key) != 20 && len(op.Key) != 52) {
			return nil, fmt.Errorf("feed requires account/storage writes: %x", op.Key)
		}
		addr := string(op.Key[:20])
		at, ok := index[addr]
		if !ok {
			at = len(feed.Accounts)
			index[addr] = at
			feed.Accounts = append(feed.Accounts, commitment.FeedAccount{Hash: keccak.Sum256(op.Key[:20])})
		}
		account := &feed.Accounts[at]
		if len(op.Key) == 20 {
			account.Update = Update(op)
			continue
		}
		slot := commitment.FeedSlot{Hash: keccak.Sum256(op.Key[20:])}
		if !op.Delete {
			slot.Value = append([]byte(nil), op.Storage...)
		}
		account.Slots = append(account.Slots, slot)
	}
	return feed, nil
}
