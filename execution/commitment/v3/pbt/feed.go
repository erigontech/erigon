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
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TranslateFeed(feed *commitment.PBinFeed) ([]Op, error) {
	var ops []Op
	if feed == nil {
		return nil, fmt.Errorf("pbin: nil feed")
	}
	emitter := NewFeedOpEmitter()
	for i := range feed.Accounts {
		if err := emitter.EmitAccount(feed.Accounts[i], func(op Op) error {
			ops = append(ops, op)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	slices.SortStableFunc(ops, func(left, right Op) int {
		return bytes.Compare(opKey(left), opKey(right))
	})
	for i := 1; i < len(ops); i++ {
		if bytes.Equal(opKey(ops[i-1]), opKey(ops[i])) {
			return nil, fmt.Errorf("pbin: duplicate operation key %x", opKey(ops[i]))
		}
	}
	return ops, nil
}

func opKey(op Op) []byte {
	if len(op.Drop) != 0 {
		return op.Drop
	}
	return op.Key
}

type FeedOpEmitter struct {
	chunks map[string][eip8297.ValueLength]byte
}

func NewFeedOpEmitter() *FeedOpEmitter {
	return &FeedOpEmitter{chunks: make(map[string][eip8297.ValueLength]byte)}
}

func NewRebuildFeedOpEmitter() *FeedOpEmitter {
	return &FeedOpEmitter{}
}

func (e *FeedOpEmitter) EmitAccount(account commitment.PBinFeedAccount, emit func(Op) error) error {
	if emit == nil {
		return fmt.Errorf("pbin: nil operation emitter")
	}
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
		return nil
	}
	if account.CodeWritten {
		code, err := eip8297.AccountCode(address, account.CodeHash, account.Code)
		if err != nil {
			return err
		}
		account.Code = code
	}

	basicKey := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	if account.CodeWritten {
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
				if e.chunks != nil {
					name := string(key)
					if old, ok := e.chunks[name]; ok {
						if old != chunk {
							return fmt.Errorf("pbin: code chunk %x carries two values", key)
						}
						continue
					}
					e.chunks[name] = chunk
				}
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
		if err := e.EmitStorageSlot(address, slot, emit); err != nil {
			return err
		}
	}
	return nil
}

func (e *FeedOpEmitter) EmitStorageSlot(address []byte, slot commitment.PBinFeedSlot, emit func(Op) error) error {
	if emit == nil {
		return fmt.Errorf("pbin: nil operation emitter")
	}
	if len(address) != length.Addr {
		return fmt.Errorf("pbin: address has length %d, want %d", len(address), length.Addr)
	}
	if len(slot.Key) > length.Hash || len(slot.Value) > eip8297.ValueLength {
		return fmt.Errorf("pbin: slot key or value is too long")
	}
	key := eip8297.TreeKeyStorage(address, slot.Key)
	value := eip8297.EncodeStorageValue(slot.Value)
	return emit(Op{Key: key, Value: value})
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
		if !account.CodeWritten || len(account.Code) == 0 || eip8297.IsEmptyCodeHash(account.CodeHash) || eip8297.IsDelegation(account.Code) {
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
