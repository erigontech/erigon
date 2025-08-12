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

package snaptype_test

import (
	"testing"

	"github.com/erigontech/erigon/db/snaptype"
)

func TestEnumeration(t *testing.T) {

	if snaptype.BlobSidecars.Enum() != snaptype.CaplinEnums.BlobSidecars {
		t.Fatal("enum mismatch", snaptype.BlobSidecars, snaptype.BlobSidecars.Enum(), snaptype.CaplinEnums.BlobSidecars)
	}

	if snaptype.BeaconBlocks.Enum() != snaptype.CaplinEnums.BeaconBlocks {
		t.Fatal("enum mismatch", snaptype.BeaconBlocks, snaptype.BeaconBlocks.Enum(), snaptype.CaplinEnums.BeaconBlocks)
	}
}

func TestNames(t *testing.T) {

	if snaptype.BeaconBlocks.Name() != snaptype.CaplinEnums.BeaconBlocks.String() {
		t.Fatal("name mismatch", snaptype.BeaconBlocks, snaptype.BeaconBlocks.Name(), snaptype.CaplinEnums.BeaconBlocks.String())
	}

	if snaptype.BlobSidecars.Name() != snaptype.CaplinEnums.BlobSidecars.String() {
		t.Fatal("name mismatch", snaptype.BlobSidecars, snaptype.BlobSidecars.Name(), snaptype.CaplinEnums.BlobSidecars.String())
	}

}
