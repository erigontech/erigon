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

package migrations

// domainLargeValuesLayout wipes chaindata when its schema major version is below 8.
// v8 introduced the LargeValues two-table indirect layout (Code/RCache/Commitment:
// keysTable DupSort `bareKey -> invStep+seqID` + valsTable plain `seqID -> value`).
// The old layout cannot be migrated in place; re-syncing from snapshots is the only safe path.
var domainLargeValuesLayout = Migration{
	Name:                 "domain_large_values_layout",
	WipeDataIfMajorBelow: 8,
}
