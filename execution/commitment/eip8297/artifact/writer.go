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
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var (
	ErrUnsorted       = errors.New("pbt artifact: leaves are not strictly ordered")
	ErrInvalidLeaf    = errors.New("pbt artifact: invalid leaf")
	ErrInvalidAccount = errors.New("pbt artifact: invalid account")
)

type Writer struct {
	StorageSpillThreshold int
}

func NewWriter() *Writer { return &Writer{StorageSpillThreshold: 1 << 20} }

func WriteSnapshot(dst io.Writer, root common.Hash, leaves KVIterator) (common.Hash, error) {
	return NewWriter().Write(dst, root, leaves)
}

func (w *Writer) Write(dst io.Writer, root common.Hash, leaves KVIterator) (common.Hash, error) {
	if leaves == nil {
		return common.Hash{}, errors.New("pbt artifact: nil leaf iterator")
	}
	if w.StorageSpillThreshold <= 0 {
		w.StorageSpillThreshold = 1 << 20
	}
	var headers, codeGroups, storageGroups bytes.Buffer
	var previous []byte
	var lastZone byte
	var header *headerBuilder
	var code *groupBuilder
	var storage *storageBuilder
	var headerCount, codeCount, storageCount uint64
	flushHeader := func() error {
		if header == nil {
			return nil
		}
		encoded, err := header.encode()
		if err != nil {
			return err
		}
		_, err = headers.Write(encoded)
		headerCount++
		header = nil
		return err
	}
	flushCode := func() error {
		if code == nil {
			return nil
		}
		encoded, err := code.encodeCode()
		if err != nil {
			return err
		}
		_, err = codeGroups.Write(encoded)
		codeCount++
		code = nil
		return err
	}
	flushStorage := func() error {
		if storage == nil {
			return nil
		}
		encoded, err := storage.encode()
		if err != nil {
			return err
		}
		if err = appendSpilled(&storageGroups, encoded, w.StorageSpillThreshold); err != nil {
			return err
		}
		storageCount++
		storage = nil
		return nil
	}
	if err := leaves(func(key, value []byte) error {
		if bytes.Equal(key, previous) || (previous != nil && bytes.Compare(key, previous) < 0) {
			return ErrUnsorted
		}
		if len(value) != eip8297.ValueLength || isZero(value) {
			return fmt.Errorf("%w: key length=%d value length=%d", ErrInvalidLeaf, len(key), len(value))
		}
		if len(key) == 0 {
			return ErrInvalidLeaf
		}
		zone := key[0]
		keyLength, ok := eip8297.ZoneKeyLength(zone)
		if !ok || len(key) != keyLength {
			return fmt.Errorf("%w: key length %d for zone %#x", ErrInvalidLeaf, len(key), zone)
		}
		if previous != nil && zone < lastZone {
			return ErrUnsorted
		}
		if zone != lastZone {
			if err := flushHeader(); err != nil {
				return err
			}
			if err := flushCode(); err != nil {
				return err
			}
			if err := flushStorage(); err != nil {
				return err
			}
			lastZone = zone
		}
		switch zone {
		case eip8297.AccountZone:
			if header == nil || !bytes.Equal(header.address[:], key[1:33]) {
				if err := flushHeader(); err != nil {
					return err
				}
				header = &headerBuilder{address: common.BytesToHash(key[1:33]), values: make(map[byte][]byte)}
			}
			header.values[key[len(key)-1]] = bytes.Clone(value)
		case eip8297.CodeZone:
			if code == nil || !bytes.Equal(code.stem[:], key[1:33]) {
				if err := flushCode(); err != nil {
					return err
				}
				code = &groupBuilder{stem: common.BytesToHash(key[1:33])}
			}
			code.entries = append(code.entries, GroupEntry{Index: key[len(key)-1], Value: bytes.Clone(value)})
		case eip8297.StorageZone:
			if storage == nil || !bytes.Equal(storage.address[:], key[1:33]) {
				if err := flushStorage(); err != nil {
					return err
				}
				storage = &storageBuilder{address: common.BytesToHash(key[1:33])}
			}
			if len(storage.groups) == 0 || !bytes.Equal(storage.groups[len(storage.groups)-1].stem[:], key[33:65]) {
				storage.groups = append(storage.groups, groupBuilder{stem: common.BytesToHash(key[33:65])})
			}
			group := &storage.groups[len(storage.groups)-1]
			group.entries = append(group.entries, GroupEntry{Index: key[len(key)-1], Value: bytes.Clone(value)})
		default:
			return ErrInvalidLeaf
		}
		previous = bytes.Clone(key)
		return nil
	}); err != nil {
		return common.Hash{}, err
	}
	if err := flushHeader(); err != nil {
		return common.Hash{}, err
	}
	if err := flushCode(); err != nil {
		return common.Hash{}, err
	}
	if err := flushStorage(); err != nil {
		return common.Hash{}, err
	}
	artifact := make([]byte, 0, 56+headers.Len()+codeGroups.Len()+storageGroups.Len())
	artifact = append(artifact, root[:]...)
	artifact = append(artifact, make([]byte, 8)...)
	binary.BigEndian.PutUint64(artifact[32:40], headerCount)
	artifact = append(artifact, headers.Bytes()...)
	artifact = append(artifact, make([]byte, 8)...)
	binary.BigEndian.PutUint64(artifact[len(artifact)-8:], codeCount)
	artifact = append(artifact, codeGroups.Bytes()...)
	artifact = append(artifact, make([]byte, 8)...)
	binary.BigEndian.PutUint64(artifact[len(artifact)-8:], storageCount)
	artifact = append(artifact, storageGroups.Bytes()...)
	if _, err := dst.Write(artifact); err != nil {
		return common.Hash{}, err
	}
	return common.Hash(keccak.Sum256(artifact)), nil
}

type headerBuilder struct {
	address common.Hash
	values  map[byte][]byte
}

func (h *headerBuilder) encode() ([]byte, error) {
	basic, ok := h.values[eip8297.BasicDataLeafKey]
	if !ok || len(basic) != eip8297.ValueLength {
		return nil, fmt.Errorf("%w: missing BASIC_DATA", ErrInvalidAccount)
	}
	codeSize := binary.BigEndian.Uint32(basic[eip8297.BasicDataCodeSizeOffset:])
	nonce := basic[eip8297.BasicDataNonceOffset : eip8297.BasicDataNonceOffset+8]
	balance := basic[eip8297.BasicDataBalanceOffset : eip8297.BasicDataBalanceOffset+16]
	if codeSize == 0 && isZero(nonce) && isZero(balance) {
		return nil, fmt.Errorf("%w: zero nonce and balance", ErrInvalidAccount)
	}
	codeHashValue, hasCodeHash := h.values[eip8297.CodeHashLeafKey]
	delegation, hasDelegation := h.values[eip8297.DelegationLeafKey]
	var kind byte
	var codeRef []byte
	switch {
	case hasDelegation:
		if hasCodeHash || len(delegation) != eip8297.ValueLength || !bytes.Equal(delegation[:3], eip8297.DelegationMarker[:]) || codeSize != eip8297.DelegationCodeLength {
			return nil, fmt.Errorf("%w: invalid delegation", ErrInvalidAccount)
		}
		kind = 2
		codeRef = bytes.Clone(delegation[3:23])
	case hasCodeHash && !bytes.Equal(codeHashValue, empty.CodeHash[:]):
		if len(codeHashValue) != eip8297.ValueLength || codeSize == 0 {
			return nil, fmt.Errorf("%w: invalid code reference", ErrInvalidAccount)
		}
		kind = 1
		codeRef = append(bytes.Clone(codeHashValue), encodeInteger(uint64(codeSize))...)
	default:
		if codeSize != 0 {
			return nil, fmt.Errorf("%w: missing code hash", ErrInvalidAccount)
		}
	}
	encoded := make([]byte, 0, 64)
	encoded = append(encoded, h.address[:]...)
	encoded = append(encoded, encodeIntegerBytes(nonce)...)
	encoded = append(encoded, encodeIntegerBytes(balance)...)
	encoded = append(encoded, kind)
	encoded = append(encoded, codeRef...)
	slots := make([]byte, 0, len(h.values))
	for sub, value := range h.values {
		if sub < eip8297.HeaderStorageOffset {
			continue
		}
		if sub >= eip8297.HeaderStorageOffset+eip8297.HeaderStorageSlots {
			return nil, fmt.Errorf("%w: header slot %d", ErrInvalidAccount, sub)
		}
		if isZero(value) {
			return nil, fmt.Errorf("%w: zero header slot", ErrInvalidAccount)
		}
		slots = append(slots, sub)
	}
	for i := 1; i < len(slots); i++ {
		for j := i; j > 0 && slots[j] < slots[j-1]; j-- {
			slots[j], slots[j-1] = slots[j-1], slots[j]
		}
	}
	encoded = append(encoded, byte(len(slots)))
	for _, sub := range slots {
		encoded = append(encoded, sub-eip8297.HeaderStorageOffset)
		encoded = append(encoded, encodeIntegerBytes(h.values[sub])...)
	}
	return encoded, nil
}

type groupBuilder struct {
	stem    common.Hash
	entries []GroupEntry
}

func (g *groupBuilder) encode() ([]byte, error) {
	return g.encodeCode()
}

func (g *groupBuilder) encodeCode() ([]byte, error) {
	if len(g.entries) == 0 || len(g.entries) > eip8297.StemSubtreeWidth {
		return nil, fmt.Errorf("%w: group entry count %d", ErrInvalidLeaf, len(g.entries))
	}
	encoded := append([]byte(nil), g.stem[:]...)
	encoded = append(encoded, byte(len(g.entries)-1))
	previous := byte(0)
	for i, entry := range g.entries {
		if i != 0 && entry.Index <= previous {
			return nil, ErrUnsorted
		}
		if isZero(entry.Value) {
			return nil, fmt.Errorf("%w: zero group value", ErrInvalidLeaf)
		}
		encoded = append(encoded, entry.Index)
		encoded = append(encoded, encodeIntegerBytes(entry.Value)...)
		previous = entry.Index
	}
	return encoded, nil
}

type storageBuilder struct {
	address common.Hash
	groups  []groupBuilder
}

func (s *storageBuilder) encode() ([]byte, error) {
	if len(s.groups) == 0 {
		return nil, ErrInvalidLeaf
	}
	encoded := append([]byte(nil), s.address[:]...)
	encoded = append(encoded, encodeInteger(uint64(len(s.groups)))...)
	for _, group := range s.groups {
		if len(group.entries) == 0 || len(group.entries) > eip8297.StemSubtreeWidth {
			return nil, ErrInvalidLeaf
		}
		encoded = append(encoded, group.stem[:]...)
		encoded = append(encoded, byte(len(group.entries)-1))
		var previous byte
		for i, entry := range group.entries {
			if i != 0 && entry.Index <= previous {
				return nil, ErrUnsorted
			}
			if isZero(entry.Value) {
				return nil, fmt.Errorf("%w: zero storage value", ErrInvalidLeaf)
			}
			encoded = append(encoded, entry.Index)
			encoded = append(encoded, encodeIntegerBytes(entry.Value)...)
			previous = entry.Index
		}
	}
	return encoded, nil
}

func appendSpilled(dst *bytes.Buffer, record []byte, threshold int) error {
	if len(record) <= threshold {
		_, err := dst.Write(record)
		return err
	}
	f, err := os.CreateTemp("", "pbt-artifact-storage-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(record); err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err == nil {
		_, err = io.Copy(dst, f)
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func encodeInteger(value uint64) []byte {
	if value == 0 {
		return []byte{0}
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	return encodeIntegerBytes(raw[:])
}

func encodeIntegerBytes(value []byte) []byte {
	value = bytes.TrimLeft(value, "\x00")
	encoded := make([]byte, 1, len(value)+1)
	encoded[0] = byte(len(value))
	return append(encoded, value...)
}

func isZero(value []byte) bool {
	return len(value) == 0 || bytes.Equal(value, make([]byte, len(value)))
}
