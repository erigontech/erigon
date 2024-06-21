package snaptype

var (
	BeaconBlocks = snapType{
		enum: CaplinEnums.BeaconBlocks,
		name: "beaconblocks",
		versions: Versions{
			Current:      1,
			MinSupported: 1,
		},
		indexes: []Index{CaplinIndexes.BeaconBlockSlot},
	}
	BlobSidecars = snapType{
		enum: CaplinEnums.BlobSidecars,
		name: "blobsidecars",
		versions: Versions{
			Current:      1,
			MinSupported: 1,
		},
		indexes: []Index{CaplinIndexes.BlobSidecarSlot},
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
