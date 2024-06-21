package snaptype_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
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
