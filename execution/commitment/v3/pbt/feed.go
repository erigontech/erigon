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
	var ops []Op
	if err := ForEachFeedOp(feed, func(op Op) error {
		ops = append(ops, op)
		return nil
	}); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(ops))
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

func ForEachFeedOp(feed *commitment.PBinFeed, emit func(Op) error) error {
	if feed == nil {
		return fmt.Errorf("pbin: nil feed")
	}
	if emit == nil {
		return fmt.Errorf("pbin: nil operation emitter")
	}
	accounts := append([]commitment.PBinFeedAccount(nil), feed.Accounts...)
	sort.SliceStable(accounts, func(i, j int) bool {
		return bytes.Compare(accounts[i].Address, accounts[j].Address) < 0
	})
	chunks := make(map[string][eip8297.ValueLength]byte)
	for i := range accounts {
		account := &accounts[i]
		if len(account.Address) != length.Addr {
			return fmt.Errorf("pbin: address has length %d, want %d", len(account.Address), length.Addr)
		}
		address := bytes.Clone(account.Address)
		cache := new(eip8297.DigestCache)
		headerPrefix := cache.AccountHeaderStem(address)
		storagePrefix := cache.AccountStoragePrefix(address)
		if !account.Exists || account.Wiped {
			if err := emit(Drop(headerPrefix)); err != nil {
				return err
			}
			if err := emit(Drop(storagePrefix)); err != nil {
				return err
			}
		}
		if !account.Exists {
			continue
		}

		basicKey := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
		if account.CodeWritten {
			if err := feedCodeHash(*account); err != nil {
				return err
			}
			basic, err := eip8297.EncodeBasicData(account.Nonce, &account.Balance, uint64(len(account.Code)))
			if err != nil {
				return err
			}
			if err := emit(Op{Key: basicKey, Value: basic}); err != nil {
				return err
			}
			if eip8297.IsDelegation(account.Code) {
				if err := emit(Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey)}); err != nil {
					return err
				}
				if err := emit(Op{Key: eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey), Value: eip8297.EncodeDelegation(account.Code)}); err != nil {
					return err
				}
			} else {
				if err := emit(Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), Value: eip8297.CodeHashValue(account.CodeHash)}); err != nil {
					return err
				}
				if err := emit(Op{Key: eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey)}); err != nil {
					return err
				}
				for index, chunk := range eip8297.ChunkifyCode(account.Code) {
					key := eip8297.TreeKeyCodeChunk(account.CodeHash, index)
					name := string(key)
					if old, ok := chunks[name]; ok {
						if old != chunk {
							return fmt.Errorf("pbin: code chunk %x carries two values", key)
						}
						continue
					}
					chunks[name] = chunk
					if err := emit(Op{Key: key, Value: chunk}); err != nil {
						return err
					}
				}
			}
		} else {
			if err := emit(Op{Key: basicKey, merge: &feedMerge{
				kind: mergeBasicData, nonce: account.Nonce, balance: account.Balance, codeHash: account.CodeHash,
			}}); err != nil {
				return err
			}
			if err := emit(Op{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), merge: &feedMerge{
				kind: mergeCodeHash, codeHash: account.CodeHash,
			}}); err != nil {
				return err
			}
		}
		for _, slot := range account.Slots {
			if len(slot.Key) > length.Hash || len(slot.Value) > eip8297.ValueLength {
				return fmt.Errorf("pbin: slot key or value is too long")
			}
			key := eip8297.TreeKeyStorage(address, slot.Key)
			value := eip8297.EncodeStorageValue(slot.Value)
			if err := emit(Op{Key: key, Value: value}); err != nil {
				return err
			}
		}
	}
	return nil
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
