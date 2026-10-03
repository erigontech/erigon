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
	"errors"
	"fmt"
	"io"

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

type SnapshotStreamCallbacks struct {
	Header  func(Header) error
	Code    func(Group) error
	Storage func(common.Hash, func(func(Group) error) error) error
}

func ReadSnapshotStreamAt(src io.ReaderAt, size int64, callbacks SnapshotStreamCallbacks) (SnapshotMeta, error) {
	var meta SnapshotMeta
	if src == nil || size < 33 {
		return meta, ErrMalformed
	}
	recordLimit := size - 33
	c := artifactCursor{src: src, limit: recordLimit}
	var headers *artifactCursor
	var previousAddress common.Hash
	var previousStem common.Hash
	var previousStorage common.Hash
	var haveStorage bool
	var storageHasGroups bool
	phase := byte(0)
	for c.offset < c.limit {
		tag, err := c.byte()
		if err != nil {
			return meta, fmt.Errorf("%w: tag: %w", ErrMalformed, err)
		}
		switch tag {
		case 0, 1, 2:
			if phase != 0 {
				return meta, fmt.Errorf("%w: header after zone", ErrMalformed)
			}
			header, err := readHeaderAt(&c, tag)
			if err != nil {
				return meta, fmt.Errorf("%w: header: %w", ErrMalformed, err)
			}
			if meta.HeaderCount != 0 && bytes.Compare(header.AddressHash[:], previousAddress[:]) <= 0 {
				return meta, ErrUnsorted
			}
			previousAddress = header.AddressHash
			meta.HeaderCount++
			if callbacks.Header != nil {
				if err := callbacks.Header(header); err != nil {
					return meta, err
				}
			}
		case 3:
			if phase == 2 {
				return meta, fmt.Errorf("%w: code group after storage", ErrMalformed)
			}
			if phase == 0 {
				phase = 1
				headers = &artifactCursor{src: src, limit: c.offset - 1}
			}
			group, err := readGroupAt(&c, 3)
			if err != nil {
				return meta, fmt.Errorf("%w: code group: %w", ErrMalformed, err)
			}
			if meta.CodeGroupCount != 0 && bytes.Compare(group.StemHash[:], previousStem[:]) <= 0 {
				return meta, ErrUnsorted
			}
			previousStem = group.StemHash
			meta.CodeGroupCount++
			if callbacks.Code != nil {
				if err := callbacks.Code(group); err != nil {
					return meta, err
				}
			}
		case 4:
			if phase == 0 {
				headers = &artifactCursor{src: src, limit: c.offset - 1}
			}
			phase = 2
			if haveStorage && !storageHasGroups {
				return meta, fmt.Errorf("%w: invalid storage account", ErrMalformed)
			}
			addressBytes, err := c.bytesCopy(32)
			if err != nil {
				return meta, fmt.Errorf("%w: storage address: %w", ErrMalformed, err)
			}
			address := common.Hash(addressBytes)
			if meta.StorageCount != 0 && bytes.Compare(address[:], previousStorage[:]) <= 0 {
				return meta, ErrUnsorted
			}
			if err := matchStorageHeader(headers, address); err != nil {
				return meta, fmt.Errorf("%w: storage header: %w", ErrMalformed, err)
			}
			previousStorage = address
			haveStorage = true
			storageHasGroups = false
			meta.StorageCount++
			groups := func(yield func(Group) error) error {
				var previousGroup common.Hash
				count := 0
				for c.offset < c.limit {
					next, err := c.byte()
					if err != nil {
						return err
					}
					if next != 5 && next != 6 {
						c.offset--
						break
					}
					group, err := readGroupAt(&c, next)
					if err != nil {
						return err
					}
					if count != 0 && bytes.Compare(group.StemHash[:], previousGroup[:]) <= 0 {
						return ErrUnsorted
					}
					previousGroup = group.StemHash
					count++
					if yield != nil {
						if err := yield(group); err != nil {
							return err
						}
					}
				}
				if count == 0 {
					return fmt.Errorf("%w: storage account has no groups", ErrMalformed)
				}
				storageHasGroups = true
				return nil
			}
			if callbacks.Storage != nil {
				if err := callbacks.Storage(address, groups); err != nil {
					return meta, err
				}
			} else if err := groups(nil); err != nil {
				return meta, err
			}
		default:
			return meta, fmt.Errorf("%w: unknown tag %#x", ErrMalformed, tag)
		}
	}
	if haveStorage && !storageHasGroups {
		return meta, fmt.Errorf("%w: invalid storage account", ErrMalformed)
	}
	trailer := artifactCursor{src: src, offset: recordLimit, limit: size}
	end, err := trailer.byte()
	if err != nil || end != 0x07 {
		return meta, fmt.Errorf("%w: missing end tag", ErrMalformed)
	}
	root, err := trailer.bytesCopy(32)
	if err != nil || trailer.offset != size {
		return meta, fmt.Errorf("%w: invalid root trailer", ErrMalformed)
	}
	copy(meta.Root[:], root)
	hash := keccak.NewFastKeccak()
	if _, err := io.Copy(hash, io.NewSectionReader(src, 0, size)); err != nil {
		return meta, err
	}
	meta.SnapshotDigest = common.BytesToHash(hash.Sum(nil))
	return meta, nil
}

type artifactCursor struct {
	src    io.ReaderAt
	offset int64
	limit  int64
	buffer []byte
	start  int64
	end    int64
}

func (c *artifactCursor) remaining() int64 { return c.limit - c.offset }

func (c *artifactCursor) bytes(size int) ([]byte, error) {
	if size < 0 || int64(size) > c.remaining() || size > 64<<10 {
		return nil, ErrMalformed
	}
	if c.buffer == nil {
		c.buffer = make([]byte, 64<<10)
	}
	if c.offset < c.start || c.offset+int64(size) > c.end {
		readSize := min(int64(len(c.buffer)), c.remaining())
		n, err := c.src.ReadAt(c.buffer[:readSize], c.offset)
		if err != nil && !(err == io.EOF && int64(n) == readSize) {
			return nil, ErrMalformed
		}
		c.start = c.offset
		c.end = c.offset + int64(n)
	}
	if c.offset+int64(size) > c.end {
		return nil, ErrMalformed
	}
	data := c.buffer[c.offset-c.start : c.offset-c.start+int64(size)]
	c.offset += int64(size)
	return data, nil
}

func (c *artifactCursor) bytesCopy(size int) ([]byte, error) {
	data, err := c.bytes(size)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(data), nil
}

func (c *artifactCursor) byte() (byte, error) {
	data, err := c.bytes(1)
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

func readHeaderAt(c *artifactCursor, kind byte) (Header, error) {
	var header Header
	address, err := c.bytesCopy(32)
	if err != nil {
		return header, err
	}
	copy(header.AddressHash[:], address)
	header.Kind = kind
	if header.Nonce, err = readIntegerAt(c, 8); err != nil {
		return Header{}, err
	}
	if header.Balance, err = readIntegerAt(c, 16); err != nil {
		return Header{}, err
	}
	switch kind {
	case 0:
		if len(header.Nonce) == 0 && len(header.Balance) == 0 {
			return Header{}, ErrInvalidAccount
		}
	case 1:
		codeHash, err := c.bytesCopy(32)
		if err != nil {
			return Header{}, err
		}
		copy(header.CodeHash[:], codeHash)
		header.CodeSize, err = readIntegerAt(c, 4)
		if err != nil || len(header.CodeSize) == 0 {
			return Header{}, fmt.Errorf("%w: invalid code size", ErrInvalidAccount)
		}
	case 2:
		target, err := c.bytesCopy(20)
		if err != nil {
			return Header{}, err
		}
		copy(header.Target[:], target)
	default:
		return Header{}, fmt.Errorf("%w: unknown account kind %d", ErrMalformed, kind)
	}
	slotCount, err := c.byte()
	if err != nil || int64(slotCount)*3 > c.remaining() {
		return Header{}, ErrMalformed
	}
	header.Slots = make([]GroupEntry, 0, int(slotCount))
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
		header.Slots = append(header.Slots, GroupEntry{Index: index, Value: value})
		previous = index
	}
	return header, nil
}

func readGroupAt(c *artifactCursor, tag byte) (Group, error) {
	var group Group
	stem, err := c.bytesCopy(32)
	if err != nil {
		return group, err
	}
	copy(group.StemHash[:], stem)
	entries := 1
	if tag != 5 {
		count, err := c.byte()
		if err != nil || tag == 6 && count == 0 {
			return Group{}, fmt.Errorf("%w: invalid multi-leaf group", ErrMalformed)
		}
		entries = int(count) + 1
	}
	if int64(entries)*3 > c.remaining() {
		return Group{}, ErrMalformed
	}
	group.Entries = make([]GroupEntry, 0, entries)
	var previous byte
	for i := range entries {
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

func readIntegerAt(c *artifactCursor, width int) ([]byte, error) {
	length, err := c.byte()
	if err != nil || int(length) > width {
		return nil, ErrMalformed
	}
	value, err := c.bytesCopy(int(length))
	if err != nil {
		return nil, err
	}
	if len(value) != 0 && value[0] == 0 {
		return nil, fmt.Errorf("%w: leading zero", ErrMalformed)
	}
	return value, nil
}

func matchStorageHeader(headers *artifactCursor, address common.Hash) error {
	for headers.offset < headers.limit {
		tag, err := headers.byte()
		if err != nil {
			return err
		}
		header, err := readHeaderAt(headers, tag)
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
