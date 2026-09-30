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
	"slices"

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

type Writer struct{}

type SnapshotRoot func() (common.Hash, error)

type artifactWriter struct {
	dst  io.Writer
	hash io.Writer
}

func NewWriter() *Writer { return &Writer{} }

func WriteSnapshot(dst io.Writer, root common.Hash, leaves KVIterator) (common.Hash, error) {
	return NewWriter().Write(dst, root, leaves)
}

func WriteSnapshotStream(dst io.Writer, leaves KVIterator, root SnapshotRoot) (common.Hash, error) {
	return NewWriter().WriteStream(dst, leaves, root)
}

func (w *Writer) Write(dst io.Writer, root common.Hash, leaves KVIterator) (common.Hash, error) {
	return w.WriteStream(dst, leaves, func() (common.Hash, error) { return root, nil })
}

func (w *Writer) WriteStream(dst io.Writer, leaves KVIterator, root SnapshotRoot) (common.Hash, error) {
	if dst == nil || leaves == nil || root == nil {
		return common.Hash{}, errors.New("pbt artifact: missing writer input")
	}
	hash := keccak.NewFastKeccak()
	output := &artifactWriter{dst: dst, hash: hash}
	write := func(data []byte) error {
		if n, err := output.Write(data); err != nil {
			return err
		} else if n != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	var previous []byte
	var zone byte
	var haveZone bool
	var header *headerBuilder
	var code *groupBuilder
	var storage *storageBuilder
	flushHeader := func() error {
		if header == nil {
			return nil
		}
		encoded, err := header.encode()
		if err == nil {
			err = write(encoded)
		}
		header = nil
		return err
	}
	flushCode := func() error {
		if code == nil {
			return nil
		}
		encoded, err := code.encodeCode()
		if err == nil {
			err = write(encoded)
		}
		code = nil
		return err
	}
	flushStorageGroup := func() error {
		if storage == nil {
			return nil
		}
		return storage.flushGroup(write)
	}
	flushStorage := func() error {
		if storage == nil {
			return nil
		}
		if err := flushStorageGroup(); err != nil {
			return err
		}
		if storage.groupCount == 0 {
			return ErrInvalidLeaf
		}
		storage = nil
		return nil
	}
	flushZone := func() error {
		if err := flushHeader(); err != nil {
			return err
		}
		if err := flushCode(); err != nil {
			return err
		}
		return flushStorage()
	}
	err := leaves(func(key, value []byte) error {
		if previous != nil && bytes.Compare(key, previous) <= 0 {
			return ErrUnsorted
		}
		if len(value) != eip8297.ValueLength || isZero(value) {
			return fmt.Errorf("%w: key length=%d value length=%d", ErrInvalidLeaf, len(key), len(value))
		}
		if len(key) == 0 {
			return ErrInvalidLeaf
		}
		currentZone := key[0]
		keyLength, ok := eip8297.ZoneKeyLength(currentZone)
		if !ok || len(key) != keyLength {
			return fmt.Errorf("%w: key length %d for zone %#x", ErrInvalidLeaf, len(key), currentZone)
		}
		if haveZone && currentZone < zone {
			return ErrUnsorted
		}
		if !haveZone || currentZone != zone {
			if err := flushZone(); err != nil {
				return err
			}
			zone, haveZone = currentZone, true
		}
		switch currentZone {
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
				if err := write(append([]byte{0x04}, storage.address[:]...)); err != nil {
					return err
				}
			}
			if storage.current != nil && storage.current.stem != common.BytesToHash(key[33:65]) {
				if err := flushStorageGroup(); err != nil {
					return err
				}
			}
			if err := storage.add(common.BytesToHash(key[33:65]), key[len(key)-1], value); err != nil {
				return err
			}
		default:
			return ErrInvalidLeaf
		}
		previous = bytes.Clone(key)
		return nil
	})
	if err != nil {
		return common.Hash{}, err
	}
	if err := flushZone(); err != nil {
		return common.Hash{}, err
	}
	rootHash, err := root()
	if err != nil {
		return common.Hash{}, err
	}
	if err := write([]byte{0x07}); err != nil {
		return common.Hash{}, err
	}
	if err := write(rootHash[:]); err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(hash.Sum(nil)), nil
}

func (w *artifactWriter) Write(data []byte) (int, error) {
	n, err := w.dst.Write(data)
	if n > 0 {
		_, _ = w.hash.Write(data[:n])
	}
	return n, err
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
	encoded = append(encoded, kind)
	encoded = append(encoded, h.address[:]...)
	encoded = append(encoded, encodeIntegerBytes(nonce)...)
	encoded = append(encoded, encodeIntegerBytes(balance)...)
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
	slices.Sort(slots)
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

func (g *groupBuilder) encodeCode() ([]byte, error) {
	if len(g.entries) == 0 || len(g.entries) > eip8297.StemSubtreeWidth {
		return nil, fmt.Errorf("%w: group entry count %d", ErrInvalidLeaf, len(g.entries))
	}
	encoded := append([]byte{0x03}, g.stem[:]...)
	encoded = append(encoded, byte(len(g.entries)-1))
	return appendGroupEntries(encoded, g.entries)
}

func appendGroupEntries(encoded []byte, entries []GroupEntry) ([]byte, error) {
	var previous byte
	for i, entry := range entries {
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
	address    common.Hash
	current    *groupBuilder
	groupCount uint64
}

func (s *storageBuilder) add(stem common.Hash, index byte, value []byte) error {
	if s.current == nil {
		s.current = &groupBuilder{stem: stem}
	} else if s.current.stem != stem {
		if bytes.Compare(stem[:], s.current.stem[:]) <= 0 {
			return ErrUnsorted
		}
		s.current = &groupBuilder{stem: stem}
	}
	if len(s.current.entries) >= eip8297.StemSubtreeWidth {
		return ErrInvalidLeaf
	}
	s.current.entries = append(s.current.entries, GroupEntry{Index: index, Value: bytes.Clone(value)})
	return nil
}

func (s *storageBuilder) flushGroup(write func([]byte) error) error {
	if s.current == nil {
		return nil
	}
	if len(s.current.entries) == 1 {
		entry := s.current.entries[0]
		encoded := append([]byte{0x05}, s.current.stem[:]...)
		encoded = append(encoded, entry.Index)
		encoded = append(encoded, encodeIntegerBytes(entry.Value)...)
		if err := write(encoded); err != nil {
			return err
		}
	} else {
		encoded := append([]byte{0x06}, s.current.stem[:]...)
		encoded = append(encoded, byte(len(s.current.entries)-1))
		var err error
		encoded, err = appendGroupEntries(encoded, s.current.entries)
		if err != nil {
			return err
		}
		if err := write(encoded); err != nil {
			return err
		}
	}
	s.groupCount++
	s.current = nil
	return nil
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
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}
