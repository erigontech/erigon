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

var ErrMalformed = errors.New("pbt artifact: malformed artifact")

func ReadSnapshot(src io.Reader) (Snapshot, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return Snapshot{}, err
	}
	return readSnapshot(data)
}

func Read(src io.Reader) (Snapshot, error) { return ReadSnapshot(src) }

func readSnapshot(data []byte) (Snapshot, error) {
	var snapshot Snapshot
	if len(data) < 56 {
		return snapshot, ErrMalformed
	}
	offset := 0
	copy(snapshot.Root[:], data[:32])
	offset = 32
	headerCountValue, err := readCount(data, &offset)
	if err != nil {
		return Snapshot{}, err
	}
	if headerCountValue > uint64(len(data)) {
		return Snapshot{}, ErrMalformed
	}
	headerCount := int(headerCountValue)
	snapshot.Headers = make([]Header, 0, headerCount)
	var previousAddress common.Hash
	for i := range headerCount {
		header, err := readHeader(data, &offset)
		if err != nil {
			return Snapshot{}, err
		}
		if i != 0 && bytes.Compare(header.AddressHash[:], previousAddress[:]) <= 0 {
			return Snapshot{}, ErrUnsorted
		}
		previousAddress = header.AddressHash
		snapshot.Headers = append(snapshot.Headers, header)
	}
	codeCountValue, err := readCount(data, &offset)
	if err != nil {
		return Snapshot{}, err
	}
	if codeCountValue > uint64(len(data)) {
		return Snapshot{}, ErrMalformed
	}
	codeCount := int(codeCountValue)
	snapshot.CodeGroups = make([]Group, 0, codeCount)
	var previousStem common.Hash
	for i := range codeCount {
		group, err := readGroup(data, &offset)
		if err != nil {
			return Snapshot{}, err
		}
		if i != 0 && bytes.Compare(group.StemHash[:], previousStem[:]) <= 0 {
			return Snapshot{}, ErrUnsorted
		}
		previousStem = group.StemHash
		snapshot.CodeGroups = append(snapshot.CodeGroups, group)
	}
	storageCountValue, err := readCount(data, &offset)
	if err != nil {
		return Snapshot{}, err
	}
	if storageCountValue > uint64(len(data)) {
		return Snapshot{}, ErrMalformed
	}
	storageCount := int(storageCountValue)
	snapshot.StorageGroups = make([]Storage, 0, storageCount)
	var previousStorage common.Hash
	for i := range storageCount {
		storage, err := readStorage(data, &offset)
		if err != nil {
			return Snapshot{}, err
		}
		if i != 0 && bytes.Compare(storage.AddressHash[:], previousStorage[:]) <= 0 {
			return Snapshot{}, ErrUnsorted
		}
		previousStorage = storage.AddressHash
		snapshot.StorageGroups = append(snapshot.StorageGroups, storage)
	}
	if offset != len(data) {
		return Snapshot{}, fmt.Errorf("%w: trailing bytes", ErrMalformed)
	}
	headerIndex := 0
	for _, storage := range snapshot.StorageGroups {
		for headerIndex < len(snapshot.Headers) && bytes.Compare(snapshot.Headers[headerIndex].AddressHash[:], storage.AddressHash[:]) < 0 {
			headerIndex++
		}
		if headerIndex == len(snapshot.Headers) || snapshot.Headers[headerIndex].AddressHash != storage.AddressHash {
			return Snapshot{}, fmt.Errorf("%w: storage has no header", ErrMalformed)
		}
	}
	snapshot.SnapshotDigest = common.Hash(keccak.Sum256(data))
	return snapshot, nil
}

func readCount(data []byte, offset *int) (uint64, error) {
	if *offset+8 > len(data) {
		return 0, ErrMalformed
	}
	value := binary.BigEndian.Uint64(data[*offset : *offset+8])
	*offset += 8
	return value, nil
}

func readHeader(data []byte, offset *int) (Header, error) {
	var header Header
	if *offset+32 > len(data) {
		return header, ErrMalformed
	}
	copy(header.AddressHash[:], data[*offset:*offset+32])
	*offset += 32
	var err error
	if header.Nonce, err = readInteger(data, offset, 8); err != nil {
		return Header{}, err
	}
	if header.Balance, err = readInteger(data, offset, 16); err != nil {
		return Header{}, err
	}
	if *offset >= len(data) {
		return Header{}, ErrMalformed
	}
	header.Kind = data[*offset]
	*offset++
	switch header.Kind {
	case 0:
		if len(header.Nonce) == 0 && len(header.Balance) == 0 {
			return Header{}, ErrInvalidAccount
		}
	case 1:
		if *offset+32 > len(data) {
			return Header{}, ErrMalformed
		}
		copy(header.CodeHash[:], data[*offset:*offset+32])
		*offset += 32
		header.CodeSize, err = readInteger(data, offset, 4)
		if err != nil || len(header.CodeSize) == 0 {
			return Header{}, fmt.Errorf("%w: invalid code size", ErrInvalidAccount)
		}
	case 2:
		if *offset+20 > len(data) {
			return Header{}, ErrMalformed
		}
		copy(header.Target[:], data[*offset:*offset+20])
		*offset += 20
	default:
		return Header{}, fmt.Errorf("%w: unknown account kind %d", ErrMalformed, header.Kind)
	}
	if *offset >= len(data) {
		return Header{}, ErrMalformed
	}
	slotCount := int(data[*offset])
	*offset++
	header.Slots = make([]Slot, 0, slotCount)
	var previous byte
	for i := range slotCount {
		if *offset >= len(data) {
			return Header{}, ErrMalformed
		}
		slot := data[*offset]
		*offset++
		if slot >= eip8297.HeaderStorageSlots || i != 0 && slot <= previous {
			return Header{}, fmt.Errorf("%w: header slot %d", ErrMalformed, slot)
		}
		value, err := readInteger(data, offset, eip8297.ValueLength)
		if err != nil || len(value) == 0 {
			return Header{}, fmt.Errorf("%w: invalid header slot", ErrMalformed)
		}
		header.Slots = append(header.Slots, Slot{Index: slot, Value: value})
		previous = slot
	}
	return header, nil
}

func readGroup(data []byte, offset *int) (Group, error) {
	var group Group
	if *offset+33 > len(data) {
		return group, ErrMalformed
	}
	copy(group.StemHash[:], data[*offset:*offset+32])
	*offset += 32
	count := int(data[*offset]) + 1
	*offset++
	group.Entries = make([]GroupEntry, 0, count)
	var previous byte
	for i := range count {
		if *offset >= len(data) {
			return Group{}, ErrMalformed
		}
		index := data[*offset]
		*offset++
		if i != 0 && index <= previous {
			return Group{}, ErrUnsorted
		}
		value, err := readInteger(data, offset, eip8297.ValueLength)
		if err != nil || len(value) == 0 {
			return Group{}, fmt.Errorf("%w: invalid group value", ErrMalformed)
		}
		group.Entries = append(group.Entries, GroupEntry{Index: index, Value: value})
		previous = index
	}
	return group, nil
}

func readStorage(data []byte, offset *int) (Storage, error) {
	var storage Storage
	if *offset+32 > len(data) {
		return storage, ErrMalformed
	}
	copy(storage.AddressHash[:], data[*offset:*offset+32])
	*offset += 32
	countBytes, err := readInteger(data, offset, 8)
	if err != nil || len(countBytes) == 0 {
		return Storage{}, fmt.Errorf("%w: zero storage group count", ErrMalformed)
	}
	count := integerValue(countBytes)
	if count == 0 {
		return Storage{}, fmt.Errorf("%w: zero storage group count", ErrMalformed)
	}
	storage.Groups = make([]Group, 0, count)
	var previous common.Hash
	for i := range count {
		group, err := readGroup(data, offset)
		if err != nil {
			return Storage{}, err
		}
		if i != 0 && bytes.Compare(group.StemHash[:], previous[:]) <= 0 {
			return Storage{}, ErrUnsorted
		}
		previous = group.StemHash
		storage.Groups = append(storage.Groups, group)
	}
	return storage, nil
}

func readInteger(data []byte, offset *int, width int) ([]byte, error) {
	if *offset >= len(data) {
		return nil, ErrMalformed
	}
	length := int(data[*offset])
	*offset++
	if length > width || *offset+length > len(data) {
		return nil, ErrMalformed
	}
	if length != 0 && data[*offset] == 0 {
		return nil, fmt.Errorf("%w: leading zero", ErrMalformed)
	}
	value := bytes.Clone(data[*offset : *offset+length])
	*offset += length
	return value, nil
}

func integerValue(value []byte) uint64 {
	var raw [8]byte
	copy(raw[8-len(value):], value)
	return binary.BigEndian.Uint64(raw[:])
}
