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

	"github.com/erigontech/erigon/common/crypto"
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
		encoded, _, err := reader.Read(kv.AccountsDomain, rawAddress, 1)
		if err != nil {
			return nil, err
		}
		account := commitment.PBinFeedAccount{Address: bytes.Clone(rawAddress), CodeHash: empty.CodeHash}
		if len(encoded) != 0 {
			decoded := new(accounts.Account)
			if err := accounts.DeserialiseV3(decoded, encoded); err != nil {
				return nil, fmt.Errorf("pbin: decode account %x: %w", rawAddress, err)
			}
			account.Exists = true
			account.Nonce = decoded.Nonce
			account.Balance = decoded.Balance
			account.CodeHash = decoded.CodeHash.Value()
		}
		if _, ok := wiped[address]; ok {
			account.Wiped = true
		}
		if _, ok := codeKeys[address]; ok {
			account.CodeWritten = true
			code, _, err := reader.Read(kv.CodeDomain, rawAddress, 1)
			if err != nil {
				return nil, err
			}
			if eip8297.IsEmptyCodeHash(account.CodeHash) {
				code = nil
			} else {
				if len(code) == 0 {
					return nil, fmt.Errorf("pbin: code missing for %x", rawAddress)
				}
				if crypto.Keccak256Hash(code) != account.CodeHash {
					return nil, fmt.Errorf("pbin: code hash mismatch for %x", rawAddress)
				}
			}
			account.Code = bytes.Clone(code)
		}
		account.Slots = make([]commitment.PBinFeedSlot, 0, len(slots[address]))
		sort.Slice(slots[address], func(i, j int) bool { return bytes.Compare(slots[address][i], slots[address][j]) < 0 })
		for _, slot := range slots[address] {
			composite := make([]byte, length.Addr+length.Hash)
			copy(composite, rawAddress)
			copy(composite[length.Addr:], slot)
			value, _, err := reader.Read(kv.StorageDomain, composite, 1)
			if err != nil {
				return nil, err
			}
			account.Slots = append(account.Slots, commitment.PBinFeedSlot{Key: bytes.Clone(slot), Value: bytes.Clone(value)})
		}
		feed.Accounts = append(feed.Accounts, account)
	}
	return feed, nil
}
