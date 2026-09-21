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
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

const StateMarker byte = commitment.CommitmentV4StateMarker

const stateSize = 1 + 8 + 8 + 32

var (
	ErrStateMarker = errors.New("commitment v4: invalid state variant marker")
	ErrStateSize   = errors.New("commitment v4: invalid state size")
)

func IsStateBlob(value []byte) bool {
	return len(value) > 0 && value[0] == StateMarker
}

func encodeState(root []byte, blockNum, txNum uint64, dst []byte) ([]byte, error) {
	if len(root) != 32 {
		return nil, ErrStateSize
	}
	dst = append(dst, make([]byte, stateSize)...)
	dst[len(dst)-stateSize] = StateMarker
	pos := len(dst) - stateSize + 1
	binary.BigEndian.PutUint64(dst[pos:pos+8], txNum)
	pos += 8
	binary.BigEndian.PutUint64(dst[pos:pos+8], blockNum)
	pos += 8
	copy(dst[pos:], root)
	return dst, nil
}

func decodeState(value []byte) (blockNum, txNum uint64, root []byte, err error) {
	if len(value) != stateSize {
		return 0, 0, nil, ErrStateSize
	}
	if value[0] != StateMarker {
		return 0, 0, nil, ErrStateMarker
	}
	txNum = binary.BigEndian.Uint64(value[1:9])
	blockNum = binary.BigEndian.Uint64(value[9:17])
	root = bytes.Clone(value[17:])
	return blockNum, txNum, root, nil
}

func (t *Trie) EncodeState(blockNum, txNum uint64, dst []byte) ([]byte, error) {
	if t == nil {
		return nil, errTrieReleased
	}
	root, err := t.RootHash()
	if err != nil {
		return nil, err
	}
	return encodeState(root, blockNum, txNum, dst)
}

func (t *Trie) RestoreState(value []byte) (uint64, uint64, error) {
	if t == nil {
		return 0, 0, errTrieReleased
	}
	if value == nil {
		t.root = nil
		return 0, 0, nil
	}
	blockNum, txNum, root, err := decodeState(value)
	if err != nil {
		return 0, 0, err
	}
	if bytes.Equal(root, empty.RootHash[:]) {
		t.root = nil
	} else {
		t.root = root
	}
	return blockNum, txNum, nil
}
