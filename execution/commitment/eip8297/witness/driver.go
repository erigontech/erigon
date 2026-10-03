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

package witness

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type PBinStorageWrite struct {
	Address []byte
	Slot    []byte
	Value   []byte
}

type PBinAccountUpdate struct {
	Address      []byte
	Values       map[byte][]byte
	Code         []byte
	Delegation   []byte
	ResetStorage bool
}

type PBinDriverInput struct {
	Reads    [][]byte
	Storage  []PBinStorageWrite
	Accounts []PBinAccountUpdate
	Deletes  [][]byte
}

func (t *PBinTree) Apply(input PBinDriverInput) (common.Hash, []PBinResolvedNode, error) {
	for _, key := range input.Reads {
		if _, _, err := t.Read(key); err != nil {
			return common.Hash{}, nil, err
		}
	}
	accounts := slices.Clone(input.Accounts)
	slices.SortStableFunc(accounts, func(a, b PBinAccountUpdate) int { return bytes.Compare(a.Address, b.Address) })
	for _, account := range accounts {
		if account.ResetStorage {
			if err := t.DeleteAccount(account.Address); err != nil {
				return common.Hash{}, nil, err
			}
		}
	}
	storage := slices.Clone(input.Storage)
	slices.SortStableFunc(storage, func(a, b PBinStorageWrite) int {
		if addressOrder := bytes.Compare(a.Address, b.Address); addressOrder != 0 {
			return addressOrder
		}
		return bytes.Compare(a.Slot, b.Slot)
	})
	for _, write := range storage {
		if len(write.Value) != eip8297.ValueLength {
			return common.Hash{}, nil, fmt.Errorf("pbin witness: driver value length %d, want %d", len(write.Value), eip8297.ValueLength)
		}
		key := eip8297.TreeKeyStorage(write.Address, write.Slot)
		if err := t.writeValue(key, write.Value); err != nil {
			return common.Hash{}, nil, err
		}
	}
	for _, account := range accounts {
		if err := t.applyAccountUpdate(account); err != nil {
			return common.Hash{}, nil, err
		}
	}
	deletes := slices.Clone(input.Deletes)
	slices.SortFunc(deletes, bytes.Compare)
	for _, address := range deletes {
		if err := t.DeleteAccount(address); err != nil {
			return common.Hash{}, nil, err
		}
	}
	return t.RootHash(), t.Resolved(), nil
}

func (t *PBinTree) applyAccountUpdate(account PBinAccountUpdate) error {
	for _, sub := range slices.Sorted(maps.Keys(account.Values)) {
		value := account.Values[sub]
		key := eip8297.TreeKeyAccount(account.Address, sub)
		if value == nil {
			if err := t.Delete(key); err != nil {
				return err
			}
			continue
		}
		if err := t.writeValue(key, value); err != nil {
			return err
		}
	}
	if len(account.Delegation) > 0 {
		value := eip8297.EncodeDelegation(account.Delegation)
		if err := t.Put(eip8297.TreeKeyAccount(account.Address, eip8297.DelegationLeafKey), value[:]); err != nil {
			return err
		}
		if err := t.Delete(eip8297.TreeKeyAccount(account.Address, eip8297.CodeHashLeafKey)); err != nil {
			return err
		}
	}
	if account.Code != nil {
		codeHash := common.Hash(keccak.Sum256(account.Code))
		codeHashValue := eip8297.CodeHashValue(codeHash)
		if err := t.Put(eip8297.TreeKeyAccount(account.Address, eip8297.CodeHashLeafKey), codeHashValue[:]); err != nil {
			return err
		}
		if err := t.Delete(eip8297.TreeKeyAccount(account.Address, eip8297.DelegationLeafKey)); err != nil {
			return err
		}
		for chunkID, chunk := range eip8297.ChunkifyCode(account.Code) {
			if bytes.Equal(chunk[:], make([]byte, eip8297.ValueLength)) {
				continue
			}
			key := eip8297.TreeKeyCodeChunk(codeHash, chunkID)
			if err := t.Put(key, chunk[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *PBinTree) writeValue(key, value []byte) error {
	if len(value) != eip8297.ValueLength {
		return fmt.Errorf("pbin witness: driver value length %d, want %d", len(value), eip8297.ValueLength)
	}
	var zero [eip8297.ValueLength]byte
	if bytes.Equal(value, zero[:]) {
		return t.Delete(key)
	}
	return t.Put(key, value)
}
