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

package v4

import (
	"errors"
	"fmt"
)

const (
	tagAccountNode byte = 0x40
	tagStorageNode byte = 0x41
	tagState       byte = 0x42
)

var (
	ErrInvalidKey        = errors.New("commitment v4: invalid key")
	ErrKeyPathLength     = errors.New("commitment v4: key path length is inconsistent")
	ErrV4RequiresV1Keyed = errors.New("commitment v4: requires a V1-keyed commitment domain")
)

func AccountNodeKey(path []byte, dst []byte) []byte {
	return nodeKey(tagAccountNode, nil, path, dst, nil)
}

func StorageNodeKey(addrHash [32]byte, path []byte, dst []byte) []byte {
	return nodeKey(tagStorageNode, addrHash[:], path, dst, nil)
}

func AccountRootKey() []byte {
	return AccountNodeKey(nil, nil)
}

func StorageRootKey(addrHash [32]byte) []byte {
	return StorageNodeKey(addrHash, nil, nil)
}

func StateKey() []byte {
	return []byte{tagState}
}

func ParseKey(key []byte) (tag byte, addrHash []byte, path []byte, err error) {
	if len(key) == 0 {
		return 0, nil, nil, ErrInvalidKey
	}
	tag = key[0]
	start := 1
	switch tag {
	case tagAccountNode:
	case tagStorageNode:
		if len(key) < 34 {
			return 0, nil, nil, fmt.Errorf("%w: storage key is too short", ErrInvalidKey)
		}
		addrHash = append([]byte(nil), key[1:33]...)
		start = 33
	case tagState:
		if len(key) != 1 {
			return 0, nil, nil, fmt.Errorf("%w: state key has length %d", ErrInvalidKey, len(key))
		}
		return tag, nil, nil, nil
	default:
		return 0, nil, nil, fmt.Errorf("%w: unknown tag 0x%02x", ErrInvalidKey, tag)
	}
	if len(key) <= start {
		return 0, nil, nil, fmt.Errorf("%w: missing path length", ErrInvalidKey)
	}
	count := int(key[len(key)-1])
	if count > 64 || len(key)-start-1 != packedLen(count) {
		return 0, nil, nil, ErrKeyPathLength
	}
	packed := key[start : len(key)-1]
	if count&1 == 1 && packed[len(packed)-1]&0x0f != 0 {
		return 0, nil, nil, fmt.Errorf("%w: non-zero odd path padding", ErrInvalidKey)
	}
	return tag, addrHash, unpackPath(packed, count, nil), nil
}

func AssertV1Keyed(domainKeyVersion bool) error {
	if domainKeyVersion {
		return ErrV4RequiresV1Keyed
	}
	return nil
}

func nodeKey(tag byte, addrHash, path, dst, packDst []byte) []byte {
	if len(path) > 64 {
		panic("commitment v4: path exceeds 64 nibbles")
	}
	dst = append(dst, tag)
	dst = append(dst, addrHash...)
	dst = append(dst, packPath(path, packDst)...)
	return append(dst, byte(len(path)))
}
