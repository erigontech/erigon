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

type PreimageIterator func(func(Preimage) error) error

func WritePreimages(dst io.Writer, records any) error {
	var iterate PreimageIterator
	switch value := records.(type) {
	case []Preimage:
		iterate = func(yield func(Preimage) error) error {
			for _, record := range value {
				if err := yield(record); err != nil {
					return err
				}
			}
			return nil
		}
	case PreimageIterator:
		iterate = value
	case func(func(Preimage) error) error:
		iterate = PreimageIterator(value)
	default:
		return ErrPreimages
	}
	if iterate == nil {
		return ErrPreimages
	}
	var previous common.Hash
	index := 0
	return iterate(func(record Preimage) error {
		digest := common.Hash(keccak.Sum256(record.Address[:]))
		if index != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return ErrUnsorted
		}
		previous = digest
		index++
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
		return nil
	})
}

func ReadPreimages(src io.Reader) ([]Preimage, error) {
	file, cleanup, err := spoolReader(src, "pbt-preimages-read-")
	if err != nil {
		return nil, err
	}
	defer cleanup()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	result := make([]Preimage, 0)
	err = ReadPreimagesAt(file, info.Size(), func(record Preimage) error {
		result = append(result, record)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func ReadPreimagesAt(src io.ReaderAt, size int64, yield func(Preimage) error) error {
	if src == nil || size < 0 {
		return ErrPreimages
	}
	c := artifactCursor{src: src, limit: size}
	var previous common.Hash
	index := 0
	for c.offset < c.limit {
		address, err := c.bytes(20)
		if err != nil {
			return fmt.Errorf("%w: truncated record", ErrPreimages)
		}
		countBytes, err := c.bytes(4)
		if err != nil {
			return fmt.Errorf("%w: truncated record", ErrPreimages)
		}
		count := binary.BigEndian.Uint32(countBytes)
		if uint64(count) > uint64(c.remaining()/32) {
			return fmt.Errorf("%w: truncated slots", ErrPreimages)
		}
		var record Preimage
		copy(record.Address[:], address)
		digest := common.Hash(keccak.Sum256(record.Address[:]))
		if index != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return ErrUnsorted
		}
		previous = digest
		index++
		record.Slots = make([][32]byte, 0, int(count))
		var previousSlot common.Hash
		for i := range count {
			slotBytes, err := c.bytes(32)
			if err != nil {
				return fmt.Errorf("%w: truncated slots", ErrPreimages)
			}
			var slot [32]byte
			copy(slot[:], slotBytes)
			slotDigest := common.Hash(keccak.Sum256(slot[:]))
			if i != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
				return ErrUnsorted
			}
			previousSlot = slotDigest
			record.Slots = append(record.Slots, slot)
		}
		if yield != nil {
			if err := yield(record); err != nil {
				return err
			}
		}
	}
	return nil
}

func JoinAt(snapshot io.ReaderAt, snapshotSize int64, preimages io.ReaderAt, preimageSize int64, hashFn eip8297.HashFn, yield func(common.Address, [32]byte) error) error {
	if hashFn == nil {
		hashFn = eip8297.HashBytes
	}
	if _, err := ReadSnapshotAt(snapshot, snapshotSize, SnapshotCallbacks{}); err != nil {
		return err
	}
	headerCursor, storageCursor, headerCount, storageCount, err := snapshotCursors(snapshot, snapshotSize)
	if err != nil {
		return err
	}
	pc := preimageCursor{}
	var headerOverflow bool
	for range headerCount {
		header, err := readHeaderAt(&headerCursor)
		if err != nil {
			return err
		}
		record, ok, err := nextPreimage(preimages, preimageSize, &pc)
		if err != nil || !ok {
			return fmt.Errorf("%w: missing address", ErrPreimages)
		}
		address32 := eip8297.RightAlign32(record.Address[:])
		addressHash := hashFn(address32[:])
		if addressHash != header.AddressHash {
			if bytes.Compare(addressHash[:], header.AddressHash[:]) < 0 {
				return fmt.Errorf("%w: surplus address", ErrPreimages)
			}
			return fmt.Errorf("%w: missing address", ErrPreimages)
		}
		hasStorage, err := matchHeaderPreimage(record, header, hashFn, yield)
		if err != nil {
			return err
		}
		headerOverflow = headerOverflow || hasStorage
	}
	if _, ok, err := nextPreimage(preimages, preimageSize, &pc); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("%w: surplus address", ErrPreimages)
	}
	if storageCount == 0 && headerOverflow {
		return fmt.Errorf("%w: surplus slot", ErrPreimages)
	}
	pc = preimageCursor{}
	var current Preimage
	haveCurrent := false
	for range storageCount {
		storage, err := readStorageAt(&storageCursor)
		if err != nil {
			return err
		}
		for !haveCurrent {
			var ok bool
			current, ok, err = nextPreimage(preimages, preimageSize, &pc)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: missing slot", ErrPreimages)
			}
			address32 := eip8297.RightAlign32(current.Address[:])
			addressHash := hashFn(address32[:])
			if bytes.Compare(addressHash[:], storage.AddressHash[:]) < 0 {
				if hasStorageSlots(current, hashFn) {
					return fmt.Errorf("%w: surplus slot", ErrPreimages)
				}
				continue
			}
			if addressHash != storage.AddressHash {
				return fmt.Errorf("%w: missing slot", ErrPreimages)
			}
			haveCurrent = true
		}
		if err := matchStoragePreimage(current, storage, hashFn, yield); err != nil {
			return err
		}
		haveCurrent = false
	}
	for {
		record, ok, err := nextPreimage(preimages, preimageSize, &pc)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if hasStorageSlots(record, hashFn) {
			return fmt.Errorf("%w: surplus slot", ErrPreimages)
		}
	}
}

type preimageCursor struct {
	offset      int64
	previous    common.Hash
	hasPrevious bool
}

func nextPreimage(src io.ReaderAt, size int64, cursor *preimageCursor) (Preimage, bool, error) {
	var record Preimage
	if cursor.offset == size {
		return record, false, nil
	}
	c := artifactCursor{src: src, offset: cursor.offset, limit: size}
	address, err := c.bytes(20)
	if err != nil {
		return record, false, ErrPreimages
	}
	countBytes, err := c.bytes(4)
	if err != nil {
		return record, false, ErrPreimages
	}
	count := binary.BigEndian.Uint32(countBytes)
	if uint64(count) > uint64(c.remaining()/32) {
		return record, false, ErrPreimages
	}
	copy(record.Address[:], address)
	record.Slots = make([][32]byte, 0, int(count))
	digest := common.Hash(keccak.Sum256(record.Address[:]))
	if cursor.hasPrevious && bytes.Compare(digest[:], cursor.previous[:]) <= 0 {
		return Preimage{}, false, ErrUnsorted
	}
	cursor.previous = digest
	cursor.hasPrevious = true
	var previousSlot common.Hash
	for i := uint32(0); i < count; i++ {
		value, err := c.bytes(32)
		if err != nil {
			return Preimage{}, false, ErrPreimages
		}
		var slot [32]byte
		copy(slot[:], value)
		slotDigest := common.Hash(keccak.Sum256(slot[:]))
		if i != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
			return Preimage{}, false, ErrUnsorted
		}
		previousSlot = slotDigest
		record.Slots = append(record.Slots, slot)
	}
	cursor.offset = c.offset
	return record, true, nil
}

func matchHeaderPreimage(record Preimage, header Header, hashFn eip8297.HashFn, yield func(common.Address, [32]byte) error) (bool, error) {
	matched := [eip8297.HeaderStorageSlots]bool{}
	hasStorage := false
	for _, slot := range record.Slots {
		treeKey := treeKeyWithHash(hashFn, record.Address[:], slot[:])
		if treeKey[0] != eip8297.AccountZone {
			hasStorage = true
			continue
		}
		index := treeKey[len(treeKey)-1] - eip8297.HeaderStorageOffset
		if !containsSlot(header.Slots, index) || matched[index] {
			return false, fmt.Errorf("%w: surplus slot", ErrPreimages)
		}
		matched[index] = true
		if yield != nil {
			if err := yield(record.Address, slot); err != nil {
				return false, err
			}
		}
	}
	for _, slot := range header.Slots {
		if !matched[slot.Index] {
			return false, fmt.Errorf("%w: missing slot", ErrPreimages)
		}
	}
	return hasStorage, nil
}

func hasStorageSlots(record Preimage, hashFn eip8297.HashFn) bool {
	for _, slot := range record.Slots {
		if treeKeyWithHash(hashFn, record.Address[:], slot[:])[0] == eip8297.StorageZone {
			return true
		}
	}
	return false
}

func matchStoragePreimage(record Preimage, storage Storage, hashFn eip8297.HashFn, yield func(common.Address, [32]byte) error) error {
	matched := make(map[string]bool)
	for _, slot := range record.Slots {
		treeKey := treeKeyWithHash(hashFn, record.Address[:], slot[:])
		if treeKey[0] != eip8297.StorageZone {
			continue
		}
		key := string(treeKey[33:])
		if matched[key] || !containsGroupEntry(storage.Groups, treeKey[33:65], treeKey[65]) {
			return fmt.Errorf("%w: surplus slot", ErrPreimages)
		}
		matched[key] = true
		if yield != nil {
			if err := yield(record.Address, slot); err != nil {
				return err
			}
		}
	}
	for _, group := range storage.Groups {
		for _, entry := range group.Entries {
			if !matched[string(append(bytes.Clone(group.StemHash[:]), entry.Index))] {
				return fmt.Errorf("%w: missing slot", ErrPreimages)
			}
		}
	}
	return nil
}

func snapshotCursors(src io.ReaderAt, size int64) (artifactCursor, artifactCursor, uint64, uint64, error) {
	c := artifactCursor{src: src, limit: size}
	if _, err := c.bytes(32); err != nil {
		return artifactCursor{}, artifactCursor{}, 0, 0, err
	}
	headerCount, err := c.count()
	if err != nil {
		return artifactCursor{}, artifactCursor{}, 0, 0, err
	}
	headerStart := c.offset
	for i := uint64(0); i < headerCount; i++ {
		if _, err := readHeaderAt(&c); err != nil {
			return artifactCursor{}, artifactCursor{}, 0, 0, err
		}
	}
	headerEnd := c.offset
	codeCount, err := c.count()
	if err != nil {
		return artifactCursor{}, artifactCursor{}, 0, 0, err
	}
	for i := uint64(0); i < codeCount; i++ {
		if _, err := readGroupAt(&c); err != nil {
			return artifactCursor{}, artifactCursor{}, 0, 0, err
		}
	}
	storageCount, err := c.count()
	if err != nil {
		return artifactCursor{}, artifactCursor{}, 0, 0, err
	}
	return artifactCursor{src: src, offset: headerStart, limit: headerEnd}, c, headerCount, storageCount, nil
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
