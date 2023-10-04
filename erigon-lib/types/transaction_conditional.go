// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

type KnownAccountStorageConditions map[libcommon.Address]*KnownAccountStorageCondition

type KnownAccountStorageCondition struct {
	StorageRootHash   *libcommon.Hash
	StorageSlotHashes map[libcommon.Hash]libcommon.Hash
}

func NewKnownAccountStorageConditionWithRootHash(hex string) *KnownAccountStorageCondition {
	return &KnownAccountStorageCondition{StorageRootHash: libcommon.HexToRefHash(hex)}
}

func NewKnownAccountStorageConditionWithSlotHashes(m map[string]string) *KnownAccountStorageCondition {
	res := map[libcommon.Hash]libcommon.Hash{}

	for k, v := range m {
		res[libcommon.HexToHash(k)] = libcommon.HexToHash(v)
	}

	return &KnownAccountStorageCondition{StorageSlotHashes: res}
}

func (v *KnownAccountStorageCondition) IsSingle() bool {
	return v != nil && v.StorageRootHash != nil && !v.IsStorage()
}

func (v *KnownAccountStorageCondition) IsStorage() bool {
	return v != nil && v.StorageSlotHashes != nil
}

func (v *KnownAccountStorageCondition) MarshalJSON() ([]byte, error) {
	if v.IsSingle() {
		return json.Marshal(v.StorageRootHash)
	}

	if v.IsStorage() {
		return json.Marshal(v.StorageSlotHashes)
	}

	return []byte("{}"), nil
}

func (v *KnownAccountStorageCondition) UnmarshalJSON(data []byte) error {
	var ErrKnownAccounts = errors.New("an incorrect list of knownAccounts")

	if len(data) == 0 {
		return nil
	}

	var m map[string]json.RawMessage

	err := json.Unmarshal(data, &m)
	if err != nil {
		// single Hash value case
		v.StorageRootHash = new(libcommon.Hash)

		innerErr := json.Unmarshal(data, v.StorageRootHash)
		if innerErr != nil {
			return fmt.Errorf("can't unmarshal to single value with error: %v value %q", innerErr, string(data))
		}

		return nil
	}

	res := make(map[libcommon.Hash]libcommon.Hash, len(m))

	for k, v := range m {
		// check k if it is a Hex value
		var kHash libcommon.Hash

		err = hexutility.UnmarshalFixedText("Hash", []byte(k), kHash[:])
		if err != nil {
			return fmt.Errorf("%w by key: %s with key %q and value %q", ErrKnownAccounts, err, k, string(v))
		}

		// check v if it is a Hex value
		var vHash libcommon.Hash

		err = hexutility.UnmarshalFixedText("hashTypeName", bytes.Trim(v, "\""), vHash[:])
		if err != nil {
			return fmt.Errorf("%w by value: %s with key %q and value %q", ErrKnownAccounts, err, k, string(v))
		}

		res[kHash] = vHash
	}

	v.StorageSlotHashes = res

	return nil
}

func InsertKnownAccounts[T libcommon.Hash | map[libcommon.Hash]libcommon.Hash](accounts KnownAccountStorageConditions, k libcommon.Address, v T) {
	switch typedV := any(v).(type) {
	case libcommon.Hash:
		accounts[k] = &KnownAccountStorageCondition{StorageRootHash: &typedV}
	case map[libcommon.Hash]libcommon.Hash:
		accounts[k] = &KnownAccountStorageCondition{StorageSlotHashes: typedV}
	}
}

type TransactionConditions struct {
	KnownAccountStorageConditions KnownAccountStorageConditions `json:"knownAccounts"`
	BlockNumberMin                *big.Int                      `json:"blockNumberMin"`
	BlockNumberMax                *big.Int                      `json:"blockNumberMax"`
	TimestampMin                  *uint64                       `json:"timestampMin"`
	TimestampMax                  *uint64                       `json:"timestampMax"`
}

func (ka KnownAccountStorageConditions) CountStorageEntries() int {
	if ka == nil {
		return 0
	}

	length := 0

	for _, v := range ka {
		// check if the value is hex string or an object
		if v.IsSingle() {
			length += 1
		} else {
			length += len(v.StorageSlotHashes)
		}
	}

	return length
}

func BigIntIsWithinRange(num *big.Int, min *big.Int, max *big.Int) (bool, error) {
	if min != nil && num.Cmp(min) == -1 {
		return false, fmt.Errorf("provided number %v is less than minimum number: %v", num, min)
	}

	if max != nil && num.Cmp(max) == 1 {
		return false, fmt.Errorf("provided number %v is greater than maximum number: %v", num, max)
	}

	return true, nil
}

func Uint64IsWithinRange(num *uint64, min *uint64, max *uint64) (bool, error) {
	if min != nil && *num < *min {
		return false, fmt.Errorf("provided number %v is less than minimum number: %v", num, min)
	}

	if max != nil && *num > *max {
		return false, fmt.Errorf("provided number %v is greater than maximum number: %v", num, max)
	}

	return true, nil
}
