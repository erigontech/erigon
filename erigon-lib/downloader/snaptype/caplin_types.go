package snaptype

var (
	BeaconBlocks = snapType{
		enum: Enums.BeaconBlocks,
		versions: Versions{
			Current:      1,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.BeaconBlockSlot},
	}
	BlobSidecars = snapType{
		enum: Enums.BlobSidecars,
		versions: Versions{
			Current:      1,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.BlobSidecarSlot},
	}

	CaplinSnapshotTypes = []Type{BeaconBlocks}
)
