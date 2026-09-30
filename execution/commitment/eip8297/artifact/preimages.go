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

package artifact

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var ErrPreimages = errors.New("pbt artifact: invalid preimages")

func WritePreimages(dst io.Writer, records []Preimage) error {
	var previous common.Hash
	for i, record := range records {
		digest := common.Hash(keccak.Sum256(record.Address[:]))
		if i != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return ErrUnsorted
		}
		previous = digest
		if len(record.Slots) > int(^uint32(0)) {
			return ErrPreimages
		}
		if _, err := dst.Write(record.Address[:]); err != nil {
			return err
		}
		var count [4]byte
		binary.BigEndian.PutUint32(count[:], uint32(len(record.Slots)))
		if _, err := dst.Write(count[:]); err != nil {
			return err
		}
		var previousSlot common.Hash
		for j, slot := range record.Slots {
			slotDigest := common.Hash(keccak.Sum256(slot[:]))
			if j != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
				return ErrUnsorted
			}
			previousSlot = slotDigest
			if _, err := dst.Write(slot[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func ReadPreimages(src io.Reader) ([]Preimage, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	offset := 0
	result := make([]Preimage, 0)
	var previous common.Hash
	for offset < len(data) {
		if len(data)-offset < 24 {
			return nil, fmt.Errorf("%w: truncated record", ErrPreimages)
		}
		var record Preimage
		copy(record.Address[:], data[offset:offset+20])
		offset += 20
		digest := common.Hash(keccak.Sum256(record.Address[:]))
		if len(result) != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return nil, ErrUnsorted
		}
		previous = digest
		count := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if uint64(count) > uint64((len(data)-offset)/32) {
			return nil, fmt.Errorf("%w: truncated slots", ErrPreimages)
		}
		record.Slots = make([][32]byte, count)
		var previousSlot common.Hash
		for i := range record.Slots {
			copy(record.Slots[i][:], data[offset:offset+32])
			offset += 32
			slotDigest := common.Hash(keccak.Sum256(record.Slots[i][:]))
			if i != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
				return nil, ErrUnsorted
			}
			previousSlot = slotDigest
		}
		result = append(result, record)
	}
	return result, nil
}

func Join(snapshot Snapshot, records []Preimage, hashFn eip8297.HashFn) error {
	if hashFn == nil {
		hashFn = eip8297.HashBytes
	}
	headerByHash := make(map[common.Hash]*Header, len(snapshot.Headers))
	for i := range snapshot.Headers {
		headerByHash[snapshot.Headers[i].AddressHash] = &snapshot.Headers[i]
	}
	storageByHash := make(map[common.Hash]*Storage, len(snapshot.StorageGroups))
	for i := range snapshot.StorageGroups {
		storageByHash[snapshot.StorageGroups[i].AddressHash] = &snapshot.StorageGroups[i]
	}
	seen := make(map[common.Hash]bool, len(records))
	for _, record := range records {
		address32 := eip8297.RightAlign32(record.Address[:])
		addressHash := hashFn(address32[:])
		if seen[addressHash] {
			return fmt.Errorf("%w: duplicate address", ErrPreimages)
		}
		seen[addressHash] = true
		header, ok := headerByHash[addressHash]
		if !ok {
			return fmt.Errorf("%w: surplus address", ErrPreimages)
		}
		storage := storageByHash[addressHash]
		matchedHeaders := make(map[byte]bool, len(header.Slots))
		matchedStorage := make(map[string]bool)
		for _, slot := range record.Slots {
			treeKey := treeKeyWithHash(hashFn, record.Address[:], slot[:])
			if treeKey[0] == eip8297.AccountZone {
				index := treeKey[len(treeKey)-1] - eip8297.HeaderStorageOffset
				if !containsSlot(header.Slots, index) {
					return fmt.Errorf("%w: surplus slot", ErrPreimages)
				}
				if matchedHeaders[index] {
					return fmt.Errorf("%w: duplicate slot", ErrPreimages)
				}
				matchedHeaders[index] = true
				continue
			}
			storageKey := string(treeKey[33:])
			if storage == nil || !containsGroupEntry(storage.Groups, treeKey[33:65], treeKey[65]) || matchedStorage[storageKey] {
				return fmt.Errorf("%w: surplus slot", ErrPreimages)
			}
			matchedStorage[storageKey] = true
		}
		for _, slot := range header.Slots {
			if !matchedHeaders[slot.Index] {
				return fmt.Errorf("%w: missing slot", ErrPreimages)
			}
		}
		if storage != nil {
			for _, group := range storage.Groups {
				for _, entry := range group.Entries {
					if !matchedStorage[string(append(bytes.Clone(group.StemHash[:]), entry.Index))] {
						return fmt.Errorf("%w: missing slot", ErrPreimages)
					}
				}
			}
		}
	}
	if len(seen) != len(headerByHash) {
		return fmt.Errorf("%w: missing address", ErrPreimages)
	}
	return nil
}

func treeKeyWithHash(hashFn eip8297.HashFn, address, slot []byte) []byte {
	address32 := eip8297.RightAlign32(address)
	slot32 := eip8297.RightAlign32(slot)
	stem := hashFn(address32[:])
	if eip8297.SlotInHeader(&slot32) {
		return eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.HeaderStorageOffset+slot32[31])
	}
	groupInput := make([]byte, 0, 64)
	groupInput = append(groupInput, address32[:]...)
	groupInput = append(groupInput, 0)
	groupInput = append(groupInput, slot32[:31]...)
	group := hashFn(groupInput)
	position := make([]byte, 0, 64)
	position = append(position, stem[:]...)
	position = append(position, group[:]...)
	return eip8297.TreeKey(eip8297.StorageZone, position, slot32[31])
}

func containsSlot(slots []Slot, index byte) bool {
	for _, slot := range slots {
		if slot.Index == index {
			return true
		}
	}
	return false
}

func containsGroupEntry(groups []Group, stem []byte, index byte) bool {
	for _, group := range groups {
		if !bytes.Equal(group.StemHash[:], stem) {
			continue
		}
		for _, entry := range group.Entries {
			if entry.Index == index {
				return true
			}
		}
	}
	return false
}
