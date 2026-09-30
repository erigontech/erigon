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
	"os"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var ErrMalformed = errors.New("pbt artifact: malformed artifact")

type SnapshotMeta struct {
	Root           common.Hash
	HeaderCount    uint64
	CodeGroupCount uint64
	StorageCount   uint64
	SnapshotDigest common.Hash
}

type SnapshotCallbacks struct {
	Header  func(Header) error
	Code    func(Group) error
	Storage func(Storage) error
}

func ReadSnapshotAt(src io.ReaderAt, size int64, callbacks SnapshotCallbacks) (SnapshotMeta, error) {
	var meta SnapshotMeta
	if src == nil || size < 56 {
		return meta, ErrMalformed
	}
	c := artifactCursor{src: src, limit: size}
	root, err := c.bytes(32)
	if err != nil {
		return meta, err
	}
	copy(meta.Root[:], root)
	headerCount, err := c.count()
	if err != nil {
		return meta, err
	}
	if err := countFits(headerCount, c.remaining(), 36); err != nil {
		return meta, err
	}
	headerStart := c.offset
	var previousAddress common.Hash
	for i := uint64(0); i < headerCount; i++ {
		header, err := readHeaderAt(&c)
		if err != nil {
			return meta, err
		}
		if i != 0 && bytes.Compare(header.AddressHash[:], previousAddress[:]) <= 0 {
			return meta, ErrUnsorted
		}
		previousAddress = header.AddressHash
		if callbacks.Header != nil {
			if err := callbacks.Header(header); err != nil {
				return meta, err
			}
		}
	}
	headerEnd := c.offset
	codeCount, err := c.count()
	if err != nil {
		return meta, err
	}
	if err := countFits(codeCount, c.remaining(), 35); err != nil {
		return meta, err
	}
	var previousStem common.Hash
	for i := uint64(0); i < codeCount; i++ {
		group, err := readGroupAt(&c)
		if err != nil {
			return meta, err
		}
		if i != 0 && bytes.Compare(group.StemHash[:], previousStem[:]) <= 0 {
			return meta, ErrUnsorted
		}
		previousStem = group.StemHash
		if callbacks.Code != nil {
			if err := callbacks.Code(group); err != nil {
				return meta, err
			}
		}
	}
	storageCount, err := c.count()
	if err != nil {
		return meta, err
	}
	if err := countFits(storageCount, c.remaining(), 68); err != nil {
		return meta, err
	}
	headerCursor := artifactCursor{src: src, offset: headerStart, limit: headerEnd}
	var previousStorage common.Hash
	for i := uint64(0); i < storageCount; i++ {
		storage, err := readStorageAt(&c)
		if err != nil {
			return meta, err
		}
		if i != 0 && bytes.Compare(storage.AddressHash[:], previousStorage[:]) <= 0 {
			return meta, ErrUnsorted
		}
		previousStorage = storage.AddressHash
		if err := matchStorageHeader(&headerCursor, storage.AddressHash); err != nil {
			return meta, err
		}
		if callbacks.Storage != nil {
			if err := callbacks.Storage(storage); err != nil {
				return meta, err
			}
		}
	}
	if c.offset != c.limit {
		return meta, fmt.Errorf("%w: trailing bytes", ErrMalformed)
	}
	hash := keccak.NewFastKeccak()
	if _, err := io.Copy(hash, io.NewSectionReader(src, 0, size)); err != nil {
		return meta, err
	}
	meta.HeaderCount = headerCount
	meta.CodeGroupCount = codeCount
	meta.StorageCount = storageCount
	meta.SnapshotDigest = common.BytesToHash(hash.Sum(nil))
	return meta, nil
}

func ReadSnapshot(src io.Reader) (Snapshot, error) {
	file, cleanup, err := spoolReader(src, "pbt-artifact-read-")
	if err != nil {
		return Snapshot{}, err
	}
	defer cleanup()
	info, err := file.Stat()
	if err != nil {
		return Snapshot{}, err
	}
	var snapshot Snapshot
	meta, err := ReadSnapshotAt(file, info.Size(), SnapshotCallbacks{
		Header: func(header Header) error {
			snapshot.Headers = append(snapshot.Headers, header)
			return nil
		},
		Code: func(group Group) error {
			snapshot.CodeGroups = append(snapshot.CodeGroups, group)
			return nil
		},
		Storage: func(storage Storage) error {
			snapshot.StorageGroups = append(snapshot.StorageGroups, storage)
			return nil
		},
	})
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.Root = meta.Root
	snapshot.SnapshotDigest = meta.SnapshotDigest
	return snapshot, nil
}

func Read(src io.Reader) (Snapshot, error) { return ReadSnapshot(src) }

type artifactCursor struct {
	src    io.ReaderAt
	offset int64
	limit  int64
}

func (c *artifactCursor) remaining() int64 { return c.limit - c.offset }

func (c *artifactCursor) bytes(size int) ([]byte, error) {
	if size < 0 || int64(size) > c.remaining() {
		return nil, ErrMalformed
	}
	data := make([]byte, size)
	if _, err := c.src.ReadAt(data, c.offset); err != nil {
		return nil, ErrMalformed
	}
	c.offset += int64(size)
	return data, nil
}

func (c *artifactCursor) byte() (byte, error) {
	data, err := c.bytes(1)
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

func (c *artifactCursor) count() (uint64, error) {
	data, err := c.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(data), nil
}

func countFits(count uint64, remaining int64, minimum int64) error {
	if remaining < 0 || count > uint64(remaining/minimum) {
		return ErrMalformed
	}
	return nil
}

func readHeaderAt(c *artifactCursor) (Header, error) {
	var header Header
	address, err := c.bytes(32)
	if err != nil {
		return header, err
	}
	copy(header.AddressHash[:], address)
	if header.Nonce, err = readIntegerAt(c, 8); err != nil {
		return Header{}, err
	}
	if header.Balance, err = readIntegerAt(c, 16); err != nil {
		return Header{}, err
	}
	if header.Kind, err = c.byte(); err != nil {
		return Header{}, err
	}
	switch header.Kind {
	case 0:
		if len(header.Nonce) == 0 && len(header.Balance) == 0 {
			return Header{}, ErrInvalidAccount
		}
	case 1:
		codeHash, err := c.bytes(32)
		if err != nil {
			return Header{}, err
		}
		copy(header.CodeHash[:], codeHash)
		header.CodeSize, err = readIntegerAt(c, 4)
		if err != nil || len(header.CodeSize) == 0 {
			return Header{}, fmt.Errorf("%w: invalid code size", ErrInvalidAccount)
		}
	case 2:
		target, err := c.bytes(20)
		if err != nil {
			return Header{}, err
		}
		copy(header.Target[:], target)
	default:
		return Header{}, fmt.Errorf("%w: unknown account kind %d", ErrMalformed, header.Kind)
	}
	slotCount, err := c.byte()
	if err != nil || int64(slotCount) > c.remaining()/3 {
		return Header{}, ErrMalformed
	}
	header.Slots = make([]Slot, 0, slotCount)
	var previous byte
	for i := 0; i < int(slotCount); i++ {
		index, err := c.byte()
		if err != nil {
			return Header{}, err
		}
		if index >= eip8297.HeaderStorageSlots || i != 0 && index <= previous {
			return Header{}, fmt.Errorf("%w: header slot %d", ErrMalformed, index)
		}
		value, err := readIntegerAt(c, eip8297.ValueLength)
		if err != nil || len(value) == 0 {
			return Header{}, fmt.Errorf("%w: invalid header slot", ErrMalformed)
		}
		header.Slots = append(header.Slots, Slot{Index: index, Value: value})
		previous = index
	}
	return header, nil
}

func readGroupAt(c *artifactCursor) (Group, error) {
	var group Group
	stem, err := c.bytes(32)
	if err != nil {
		return group, err
	}
	copy(group.StemHash[:], stem)
	count, err := c.byte()
	if err != nil {
		return Group{}, err
	}
	entries := int(count) + 1
	if int64(entries)*3 > c.remaining() {
		return Group{}, ErrMalformed
	}
	group.Entries = make([]GroupEntry, 0, entries)
	var previous byte
	for i := 0; i < entries; i++ {
		index, err := c.byte()
		if err != nil {
			return Group{}, err
		}
		if i != 0 && index <= previous {
			return Group{}, ErrUnsorted
		}
		value, err := readIntegerAt(c, eip8297.ValueLength)
		if err != nil || len(value) == 0 {
			return Group{}, fmt.Errorf("%w: invalid group value", ErrMalformed)
		}
		group.Entries = append(group.Entries, GroupEntry{Index: index, Value: value})
		previous = index
	}
	return group, nil
}

func readStorageAt(c *artifactCursor) (Storage, error) {
	var storage Storage
	address, err := c.bytes(32)
	if err != nil {
		return storage, err
	}
	copy(storage.AddressHash[:], address)
	countBytes, err := readIntegerAt(c, 8)
	if err != nil || len(countBytes) == 0 {
		return Storage{}, fmt.Errorf("%w: zero storage group count", ErrMalformed)
	}
	count := integerValue(countBytes)
	if count == 0 || count > uint64(c.remaining()/35) {
		return Storage{}, fmt.Errorf("%w: invalid storage group count", ErrMalformed)
	}
	storage.Groups = make([]Group, 0, int(count))
	var previous common.Hash
	for i := uint64(0); i < count; i++ {
		group, err := readGroupAt(c)
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

func readIntegerAt(c *artifactCursor, width int) ([]byte, error) {
	length, err := c.byte()
	if err != nil || int(length) > width {
		return nil, ErrMalformed
	}
	value, err := c.bytes(int(length))
	if err != nil {
		return nil, err
	}
	if len(value) != 0 && value[0] == 0 {
		return nil, fmt.Errorf("%w: leading zero", ErrMalformed)
	}
	return value, nil
}

func integerValue(value []byte) uint64 {
	var raw [8]byte
	copy(raw[8-len(value):], value)
	return binary.BigEndian.Uint64(raw[:])
}

func matchStorageHeader(headers *artifactCursor, address common.Hash) error {
	for headers.offset < headers.limit {
		header, err := readHeaderAt(headers)
		if err != nil {
			return err
		}
		if bytes.Compare(header.AddressHash[:], address[:]) >= 0 {
			if header.AddressHash != address {
				return fmt.Errorf("%w: storage has no header", ErrMalformed)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: storage has no header", ErrMalformed)
}

func spoolReader(src io.Reader, pattern string) (*os.File, func(), error) {
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		name := file.Name()
		_ = file.Close()
		_ = os.Remove(name)
	}
	if _, err := io.Copy(file, src); err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, nil, err
	}
	return file, cleanup, nil
}
