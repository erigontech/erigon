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

type recordDelta struct {
	key  []byte
	data []byte
	prev []byte
}

type putBranchFunc func(key, data, prev []byte) error

func foldAndEncodeRecord(ctx commitment.PatriciaContext, n *node, depth int, key []byte) ([32]byte, recordDelta, error) {
	hash, err := fold(n, depth)
	if err != nil {
		return [32]byte{}, recordDelta{}, err
	}
	delta, err := readRecordDelta(ctx, key, encodeRecord(n, depth, nil))
	if err != nil {
		return [32]byte{}, recordDelta{}, err
	}
	return hash, delta, nil
}

func readRecordDelta(ctx commitment.PatriciaContext, key, data []byte) (recordDelta, error) {
	prev, _, err := ctx.Branch(key)
	if err != nil {
		return recordDelta{}, err
	}
	return recordDelta{
		key:  bytes.Clone(key),
		data: bytes.Clone(data),
		prev: bytes.Clone(prev),
	}, nil
}

func applyDeltas(deltas []recordDelta, putBranch putBranchFunc) error {
	for _, delta := range deltas {
		if bytes.Equal(delta.prev, delta.data) {
			continue
		}
		if err := putBranch(delta.key, delta.data, delta.prev); err != nil {
			return err
		}
	}
	return nil
}
