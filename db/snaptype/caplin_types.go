// Copyright 2024 The Erigon Authors
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

package snaptype

import "github.com/erigontech/erigon/db/version"

var (
	BeaconBlocks = snapType{
		enum:     CaplinEnums.BeaconBlocks,
		name:     "beaconblocks",
		versions: version.V1_1_standart,
		indexes:  []Index{CaplinIndexes.BeaconBlockSlot},
	}
	BlobSidecars = snapType{
		enum:     CaplinEnums.BlobSidecars,
		name:     "blobsidecars",
		versions: version.V1_1_standart,
		indexes:  []Index{CaplinIndexes.BlobSidecarSlot},
	}

	CaplinSnapshotTypes = []Type{BeaconBlocks, BlobSidecars}
)

func IsCaplinType(t Enum) bool {

	for _, ct := range CaplinSnapshotTypes {
		if t == ct.Enum() {
			return true
		}
	}

	return false
}
