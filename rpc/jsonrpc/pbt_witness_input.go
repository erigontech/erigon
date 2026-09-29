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

package jsonrpc

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	eipWitness "github.com/erigontech/erigon/execution/commitment/eip8297/witness"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type pbinWitnessInput struct {
	eipWitness.PBinDriverInput
	Codes [][]byte
}

func buildPBinWitnessInput(rs *RecordingState) (pbinWitnessInput, error) {
	result := pbinWitnessInput{}
	readKeys := make(map[string][]byte)
	for address, source := range rs.accountReadSources {
		if source&recordingReadPreState != 0 {
			key := eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey)
			readKeys[string(key)] = key
		}
	}
	for address, keys := range rs.storageReadSources {
		for slot, source := range keys {
			if source&recordingReadPreState != 0 {
				key := eip8297.TreeKeyStorage(address[:], slot[:])
				readKeys[string(key)] = key
			}
		}
	}
	for address, code := range rs.PreStateCode {
		if eip8297.IsDelegation(code) {
			key := eip8297.TreeKeyAccount(address[:], eip8297.DelegationLeafKey)
			readKeys[string(key)] = key
			continue
		}
		codeHash := rs.codeHash(code)
		key := eip8297.TreeKeyAccount(address[:], eip8297.CodeHashLeafKey)
		readKeys[string(key)] = key
		for chunkID := range eip8297.ChunkifyCode(code) {
			chunkKey := eip8297.TreeKeyCodeChunk(codeHash, chunkID)
			readKeys[string(chunkKey)] = chunkKey
		}
	}
	result.Reads = make([][]byte, 0, len(readKeys))
	for _, key := range readKeys {
		result.Reads = append(result.Reads, key)
	}
	slices.SortFunc(result.Reads, bytes.Compare)

	for address, keys := range rs.ModifiedStorage {
		for slot := range keys {
			value := rs.storageOverlay[address][slot]
			original := rs.originalStorage[address][slot]
			if value.Eq(&original) {
				continue
			}
			encoded := value.Bytes32()
			result.Storage = append(result.Storage, eipWitness.PBinStorageWrite{
				Address: append([]byte(nil), address[:]...),
				Slot:    append([]byte(nil), slot[:]...),
				Value:   append([]byte(nil), encoded[:]...),
			})
		}
	}

	accountAddresses := make(map[common.Address]struct{}, len(rs.ModifiedAccounts))
	for address := range rs.ModifiedAccounts {
		if _, deleted := rs.DeletedAccounts[address]; !deleted {
			accountAddresses[address] = struct{}{}
		}
	}
	for address := range rs.ModifiedCode {
		if _, deleted := rs.DeletedAccounts[address]; !deleted {
			accountAddresses[address] = struct{}{}
		}
	}
	for address := range accountAddresses {
		update := eipWitness.PBinAccountUpdate{Address: append([]byte(nil), address[:]...)}
		if account, ok := rs.accountOverlay[address]; ok && account != nil {
			basic, err := pbinAccountBasicValue(rs, address, account, false)
			if err != nil {
				return pbinWitnessInput{}, fmt.Errorf("pbin witness: account %s basic data: %w", address, err)
			}
			original, originalOK := rs.originalAccounts[address]
			if !originalOK || original == nil {
				update.Values = map[byte][]byte{eip8297.BasicDataLeafKey: basic}
			} else {
				oldBasic, err := pbinAccountBasicValue(rs, address, original, true)
				if err != nil {
					return pbinWitnessInput{}, fmt.Errorf("pbin witness: account %s original basic data: %w", address, err)
				}
				if !bytes.Equal(oldBasic, basic) {
					update.Values = map[byte][]byte{eip8297.BasicDataLeafKey: basic}
				}
			}
		}
		if code, modified := rs.ModifiedCode[address]; modified {
			oldCode, err := rs.inner.ReadAccountCode(accounts.InternAddress(address))
			if err != nil || !bytes.Equal(oldCode, code) {
				if eip8297.IsDelegation(code) {
					update.Delegation = cloneNonNil(code)
				} else {
					update.Code = cloneNonNil(code)
				}
			}
		}
		if len(update.Values) != 0 || update.Code != nil || len(update.Delegation) != 0 {
			result.Accounts = append(result.Accounts, update)
		}
	}
	for address := range rs.DeletedAccounts {
		result.Deletes = append(result.Deletes, append([]byte(nil), address[:]...))
	}
	slices.SortFunc(result.Storage, func(a, b eipWitness.PBinStorageWrite) int {
		if result := bytes.Compare(a.Address, b.Address); result != 0 {
			return result
		}
		return bytes.Compare(a.Slot, b.Slot)
	})
	slices.SortFunc(result.Accounts, func(a, b eipWitness.PBinAccountUpdate) int { return bytes.Compare(a.Address, b.Address) })
	slices.SortFunc(result.Deletes, bytes.Compare)

	codeSet := make(map[string][]byte, len(rs.pbtCodeReads))
	for key, code := range rs.pbtCodeReads {
		codeSet[key] = append([]byte(nil), code...)
	}
	for _, code := range codeSet {
		if len(code) > 0 {
			result.Codes = append(result.Codes, code)
		}
	}
	slices.SortFunc(result.Codes, bytes.Compare)
	return result, nil
}

func pbinAccountBasicValue(rs *RecordingState, address common.Address, account *accounts.Account, original bool) ([]byte, error) {
	codeSize := uint64(0)
	if !original {
		if code, changed := rs.ModifiedCode[address]; changed {
			codeSize = uint64(len(code))
		} else {
			size, err := rs.inner.ReadAccountCodeSize(accounts.InternAddress(address))
			if err != nil {
				return nil, err
			}
			codeSize = uint64(size)
		}
	} else {
		code, err := rs.inner.ReadAccountCode(accounts.InternAddress(address))
		if err != nil {
			return nil, err
		}
		codeSize = uint64(len(code))
	}
	value, err := eip8297.EncodeBasicData(account.Nonce, &account.Balance, codeSize)
	if err != nil {
		return nil, err
	}
	return value[:], nil
}

func cloneNonNil(value []byte) []byte {
	result := make([]byte, len(value))
	copy(result, value)
	return result
}
