package snapshotsync

type PreverifiedSnapshotHashes struct {
	Segments map[string]string
}

var mainnetPreverifiedSnapshotHashes = PreverifiedSnapshotHashes{
	Segments: map[string]string{},
}

var goerliPreverifiedSnapshotHashes = PreverifiedSnapshotHashes{
	Segments: map[string]string{},
}
