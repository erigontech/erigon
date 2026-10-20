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

package cache

// Mode selects what GenericCache does when an insert would overflow capacity.
//
// ModeEvictLRU (default) drops the least-recently-used entry, matching the
// other state caches in the tree. ModeNoOp keeps the historical "first writers
// win" behaviour — once full, new keys are dropped (counted via a metric) —
// and exists only as a diagnostic baseline for the regression bench.
type Mode uint8

const (
	ModeEvictLRU Mode = iota
	ModeNoOp
)

func (m Mode) String() string {
	switch m {
	case ModeEvictLRU:
		return "evict"
	case ModeNoOp:
		return "noop"
	}
	return "unknown"
}
