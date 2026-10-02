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

package commitmentdb

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BinFeedFromState(keys, codeKeys, wiped map[string]struct{}, reader StateReader) (*commitment.PBinFeed, error) {
	if reader == nil {
		return nil, fmt.Errorf("pbin: nil state reader")
	}
	addresses := make(map[string]struct{}, len(keys)+len(codeKeys)+len(wiped))
	slots := make(map[string][][]byte)
	for key := range keys {
		raw := []byte(key)
		switch len(raw) {
		case length.Addr:
			addresses[key] = struct{}{}
		case length.Addr + length.Hash:
			address := string(raw[:length.Addr])
			addresses[address] = struct{}{}
			slots[address] = append(slots[address], bytes.Clone(raw[length.Addr:]))
		default:
			return nil, fmt.Errorf("pbin: plain key has length %d", len(raw))
		}
	}
	for key := range codeKeys {
		if len(key) != length.Addr {
			return nil, fmt.Errorf("pbin: code key has length %d", len(key))
		}
		addresses[key] = struct{}{}
	}
	for key := range wiped {
		if len(key) != length.Addr {
			return nil, fmt.Errorf("pbin: wiped key has length %d", len(key))
		}
		addresses[key] = struct{}{}
	}
	ordered := make([]string, 0, len(addresses))
	for address := range addresses {
		ordered = append(ordered, address)
	}
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare([]byte(ordered[i]), []byte(ordered[j])) < 0 })
	feed := &commitment.PBinFeed{Accounts: make([]commitment.PBinFeedAccount, 0, len(ordered))}
	for _, address := range ordered {
		rawAddress := []byte(address)
		account, err := BinFeedAccountFromState(rawAddress, slots[address], hasKey(codeKeys, address), hasKey(wiped, address), reader)
		if err != nil {
			return nil, err
		}
		feed.Accounts = append(feed.Accounts, account)
	}
	return feed, nil
}

func hasKey(keys map[string]struct{}, key string) bool {
	_, ok := keys[key]
	return ok
}

func BinFeedAccountFromState(address []byte, slotKeys [][]byte, codeWritten, wiped bool, reader StateReader) (commitment.PBinFeedAccount, error) {
	if reader == nil {
		return commitment.PBinFeedAccount{}, fmt.Errorf("pbin: nil state reader")
	}
	if len(address) != length.Addr {
		return commitment.PBinFeedAccount{}, fmt.Errorf("pbin: address has length %d, want %d", len(address), length.Addr)
	}
	encoded, _, err := reader.Read(kv.AccountsDomain, address, 1)
	if err != nil {
		return commitment.PBinFeedAccount{}, err
	}
	account := commitment.PBinFeedAccount{Address: bytes.Clone(address), CodeHash: empty.CodeHash, Wiped: wiped, CodeWritten: codeWritten}
	if len(encoded) != 0 {
		decoded := new(accounts.Account)
		if err := accounts.DeserialiseV3(decoded, encoded); err != nil {
			return commitment.PBinFeedAccount{}, fmt.Errorf("pbin: decode account %x: %w", address, err)
		}
		account.Exists = true
		account.Nonce = decoded.Nonce
		account.Balance = decoded.Balance
		account.CodeHash = decoded.CodeHash.Value()
	}
	if codeWritten {
		code, _, err := reader.Read(kv.CodeDomain, address, 1)
		if err != nil {
			return commitment.PBinFeedAccount{}, err
		}
		code, err = eip8297.AccountCode(address, account.CodeHash, code)
		if err != nil {
			return commitment.PBinFeedAccount{}, err
		}
		account.Code = bytes.Clone(code)
	}
	account.Slots = make([]commitment.PBinFeedSlot, 0, len(slotKeys))
	sort.Slice(slotKeys, func(i, j int) bool { return bytes.Compare(slotKeys[i], slotKeys[j]) < 0 })
	for _, slot := range slotKeys {
		if len(slot) != length.Hash {
			return commitment.PBinFeedAccount{}, fmt.Errorf("pbin: slot key has length %d, want %d", len(slot), length.Hash)
		}
		composite := make([]byte, length.Addr+length.Hash)
		copy(composite, address)
		copy(composite[length.Addr:], slot)
		value, _, err := reader.Read(kv.StorageDomain, composite, 1)
		if err != nil {
			return commitment.PBinFeedAccount{}, err
		}
		account.Slots = append(account.Slots, commitment.PBinFeedSlot{Key: bytes.Clone(slot), Value: bytes.Clone(value)})
	}
	return account, nil
}

func BinFeedStorageSlotFromState(address, slot []byte, reader StateReader) (commitment.PBinFeedSlot, error) {
	if reader == nil {
		return commitment.PBinFeedSlot{}, fmt.Errorf("pbin: nil state reader")
	}
	if len(address) != length.Addr {
		return commitment.PBinFeedSlot{}, fmt.Errorf("pbin: address has length %d, want %d", len(address), length.Addr)
	}
	if len(slot) != length.Hash {
		return commitment.PBinFeedSlot{}, fmt.Errorf("pbin: slot key has length %d, want %d", len(slot), length.Hash)
	}
	composite := make([]byte, length.Addr+length.Hash)
	copy(composite, address)
	copy(composite[length.Addr:], slot)
	value, _, err := reader.Read(kv.StorageDomain, composite, 1)
	if err != nil {
		return commitment.PBinFeedSlot{}, err
	}
	return commitment.PBinFeedSlot{Key: bytes.Clone(slot), Value: bytes.Clone(value)}, nil
}
