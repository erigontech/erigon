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

package execctx

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// pinBranchResolver resolves trunk-preload branch reads for the adaptive pin
// controller. Resolved values are pinned into the branch cache as the current
// branch state, so the read must be the authoritative latest (DB then files) —
// a files-only read pins stale data whenever the newest value is still in the DB.
// The TemporalGetter parameter enforces that: files-only reads are not reachable.
// Empty values (deletion tombstones) resolve to nil like absent keys: the branch
// cache never stores tombstones, and unpinned prefixes fall through to the
// authoritative read anyway.
func pinBranchResolver(ttx kv.TemporalGetter) commitment.BatchBranchResolver {
	return func(keys [][]byte) ([][]byte, error) {
		vals := make([][]byte, len(keys))
		for i, k := range keys {
			v, _, err := ttx.GetLatest(kv.CommitmentDomain, k)
			if err != nil {
				return nil, err
			}
			if len(v) > 0 {
				vals[i] = common.Copy(v)
			}
		}
		return vals, nil
	}
}
