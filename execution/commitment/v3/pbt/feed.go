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

package pbt

import (
	"bytes"
	"fmt"
	"sort"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TranslateFeed(feed *commitment.PBinFeed) ([]Op, error) {
	if feed == nil {
		return nil, fmt.Errorf("pbin: nil feed")
	}
	accounts := append([]commitment.PBinFeedAccount(nil), feed.Accounts...)
	sort.SliceStable(accounts, func(i, j int) bool {
		return bytes.Compare(accounts[i].Address, accounts[j].Address) < 0
	})
	ops := make([]Op, 0, len(accounts)*5)
	seen := make(map[string]struct{})
	chunks := make(map[string][eip8297.ValueLength]byte)
	for i := range accounts {
		account := &accounts[i]
		if len(account.Address) != length.Addr {
			return nil, fmt.Errorf("pbin: address has length %d, want %d", len(account.Address), length.Addr)
		}
		address := bytes.Clone(account.Address)
		cache := new(eip8297.DigestCache)
		headerPrefix := cache.AccountHeaderStem(address)
		storagePrefix := cache.AccountStoragePrefix(address)
		if !account.Exists || account.Wiped {
			ops = append(ops, Drop(headerPrefix), Drop(storagePrefix))
		}
		if !account.Exists {
			continue
		}

		basicKey := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
		if account.CodeWritten {
			if err := feedCodeHash(*account); err != nil {
				return nil, err
			}
			basic, err := eip8297.EncodeBasicData(account.Nonce, &account.Balance, uint64(len(account.Code)))
			if err != nil {
				return nil, err
			}
			ops = append(ops, Op{Key: basicKey, Value: basic})
			if eip8297.IsDelegation(account.Code) {
				ops = append(ops,
					Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey)},
					Op{Key: eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey), Value: eip8297.EncodeDelegation(account.Code)},
				)
			} else {
				ops = append(ops,
					Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), Value: eip8297.CodeHashValue(account.CodeHash)},
					Op{Key: eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey)},
				)
				for index, chunk := range eip8297.ChunkifyCode(account.Code) {
					key := eip8297.TreeKeyCodeChunk(account.CodeHash, index)
					name := string(key)
					if old, ok := chunks[name]; ok {
						if old != chunk {
							return nil, fmt.Errorf("pbin: code chunk %x carries two values", key)
						}
						continue
					}
					chunks[name] = chunk
					ops = append(ops, Op{Key: key, Value: chunk})
				}
			}
		} else {
			ops = append(ops,
				Op{Key: basicKey, merge: &feedMerge{
					kind: mergeBasicData, nonce: account.Nonce, balance: account.Balance, codeHash: account.CodeHash,
				}},
				Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), merge: &feedMerge{
					kind: mergeCodeHash, codeHash: account.CodeHash,
				}},
			)
		}
		for _, slot := range account.Slots {
			if len(slot.Key) > length.Hash || len(slot.Value) > eip8297.ValueLength {
				return nil, fmt.Errorf("pbin: slot key or value is too long")
			}
			key := eip8297.TreeKeyStorage(address, slot.Key)
			value := eip8297.EncodeStorageValue(slot.Value)
			ops = append(ops, Op{Key: key, Value: value})
		}
	}
	for _, op := range ops {
		key := op.Key
		if len(op.Drop) != 0 {
			key = op.Drop
		}
		name := string(key)
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("pbin: duplicate operation key %x", key)
		}
		seen[name] = struct{}{}
	}
	sort.SliceStable(ops, func(i, j int) bool {
		left, right := ops[i].Key, ops[j].Key
		if len(ops[i].Drop) != 0 {
			left = ops[i].Drop
		}
		if len(ops[j].Drop) != 0 {
			right = ops[j].Drop
		}
		return bytes.Compare(left, right) < 0
	})
	return ops, nil
}

func feedCodeHash(account commitment.PBinFeedAccount) error {
	actual := common.Hash(keccak.Sum256(account.Code))
	if eip8297.CodeHashValue(actual) != eip8297.CodeHashValue(account.CodeHash) {
		return fmt.Errorf("pbin: code hash does not match code")
	}
	return nil
}

func (t *Trie) ProcessFeed(feed *commitment.PBinFeed) (common.Hash, error) {
	ops, err := TranslateFeed(feed)
	if err != nil {
		return common.Hash{}, err
	}
	return t.Process(ops)
}

func feedCodeStats(feed *commitment.PBinFeed) commitment.PBinCodeStats {
	stats := commitment.PBinCodeStats{}
	seen := make(map[common.Hash]struct{})
	for i := range feed.Accounts {
		account := &feed.Accounts[i]
		if !account.CodeWritten || len(account.Code) == 0 || eip8297.IsDelegation(account.Code) {
			continue
		}
		stats.CodeBearingAccounts++
		if _, ok := seen[account.CodeHash]; !ok {
			seen[account.CodeHash] = struct{}{}
			stats.UniqueCodeHashes++
		}
	}
	return stats
}
