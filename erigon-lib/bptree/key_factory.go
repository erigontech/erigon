// Copyright 2022 The Erigon Authors
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

package bptree

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type KeyFactory interface {
	NewUniqueKeyValues(reader *bufio.Reader) KeyValues
	NewUniqueKeys(reader *bufio.Reader) Keys
}

type KeyBinaryFactory struct {
	keySize int
}

func NewKeyBinaryFactory(keySize int) KeyFactory {
	return &KeyBinaryFactory{keySize: keySize}
}

func (factory *KeyBinaryFactory) NewUniqueKeyValues(reader *bufio.Reader) KeyValues {
	kvPairs := factory.readUniqueKeyValues(reader)
	sort.Sort(kvPairs)
	return kvPairs
}

func (factory *KeyBinaryFactory) NewUniqueKeys(reader *bufio.Reader) Keys {
	keys := factory.readUniqueKeys(reader)
	sort.Sort(keys)
	return keys
}

func (factory *KeyBinaryFactory) readUniqueKeyValues(reader *bufio.Reader) KeyValues {
	kvPairs := KeyValues{make([]*Felt, 0), make([]*Felt, 0)}
	keyRegistry := make(map[Felt]bool)
	buffer := make([]byte, BufferSize)
	for {
		bytesRead, err := reader.Read(buffer)
		ensure(err == nil || err == io.EOF, fmt.Sprintf("readUniqueKeyValues: read error %s\n", err))
		if err == io.EOF {
			break
		}
		keyBytesCount := factory.keySize * (bytesRead / factory.keySize)
		duplicatedKeys := 0
		for i := 0; i < keyBytesCount; i += factory.keySize {
			key := factory.readKey(buffer, i)
			if _, duplicated := keyRegistry[key]; duplicated {
				duplicatedKeys++
				continue
			}
			keyRegistry[key] = true
			value := key // Shortcut: value equal to key
			kvPairs.keys = append(kvPairs.keys, &key)
			kvPairs.values = append(kvPairs.values, &value)
		}
	}
	return kvPairs
}

func (factory *KeyBinaryFactory) readUniqueKeys(reader *bufio.Reader) Keys {
	keys := make(Keys, 0)
	keyRegistry := make(map[Felt]bool)
	buffer := make([]byte, BufferSize)
	for {
		bytesRead, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		keyBytesCount := factory.keySize * (bytesRead / factory.keySize)
		duplicatedKeys := 0
		for i := 0; i < keyBytesCount; i += factory.keySize {
			key := factory.readKey(buffer, i)
			if _, duplicated := keyRegistry[key]; duplicated {
				duplicatedKeys++
				continue
			}
			keyRegistry[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}

func (factory *KeyBinaryFactory) readKey(buffer []byte, offset int) Felt {
	keySlice := buffer[offset : offset+factory.keySize]
	switch factory.keySize {
	case 1:
		return Felt(keySlice[0])
	case 2:
		return Felt(binary.BigEndian.Uint16(keySlice))
	case 4:
		return Felt(binary.BigEndian.Uint32(keySlice))
	default:
		return Felt(binary.BigEndian.Uint64(keySlice))
	}
}
