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

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

func (t *Trie) EncodeState(blockNum, txNum uint64, dst []byte) ([]byte, error) {
	root, err := t.RootHash()
	if err != nil {
		return nil, err
	}
	return commitment.EncodeCommitmentV4State(root, blockNum, txNum, dst)
}

func (t *Trie) RestoreState(value []byte) (uint64, uint64, error) {
	if value == nil {
		t.root = nil
		return 0, 0, nil
	}
	blockNum, txNum, root, err := commitment.DecodeCommitmentV4State(value)
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
