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
	"github.com/erigontech/erigon/execution/commitment"
)

type recordDelta = commitment.BranchDelta

const encodeSlack = 96

func applyDeltas(parts deltaParts, putBranch func(key, data, prev []byte) error) error {
	for _, part := range parts {
		for _, delta := range part {
			if err := putBranch(delta.Key, delta.Data, delta.Prev); err != nil {
				return err
			}
		}
	}
	return nil
}
