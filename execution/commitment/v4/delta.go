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

	"github.com/erigontech/erigon/execution/commitment"
)

type recordDelta = commitment.BranchDelta

type putBranchFunc func(key, data, prev []byte) error

func foldAndEncodeRecord(ctx commitment.PatriciaContext, n *node, depth int, key []byte) ([32]byte, recordDelta, error) {
	hash, err := fold(n, depth)
	if err != nil {
		return [32]byte{}, recordDelta{}, err
	}
	prev := n.raw
	if !n.loaded {
		stored, _, err := ctx.Branch(key)
		if err != nil {
			return [32]byte{}, recordDelta{}, err
		}
		prev = bytes.Clone(stored)
	}
	data := encodeRecord(n, depth, make([]byte, 0, len(prev)+encodeSlack))
	return hash, newRecordDelta(key, data, prev), nil
}

const encodeSlack = 96

func newRecordDelta(key, data, prev []byte) recordDelta {
	if prev == nil {
		prev = []byte{}
	}
	return recordDelta{Key: key, Data: data, Prev: prev}
}

func applyDeltas(deltas []recordDelta, putBranch putBranchFunc) error {
	for _, delta := range deltas {
		if bytes.Equal(delta.Prev, delta.Data) {
			continue
		}
		if err := putBranch(delta.Key, delta.Data, delta.Prev); err != nil {
			return err
		}
	}
	return nil
}
