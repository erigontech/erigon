package params

var (
	MainnetChainSnapshotConfig = &SnapshotsConfig{}
	GoerliChainSnapshotConfig  = &SnapshotsConfig{
		ExpectBlocks: 5_900_000 - 1,
	}
)

type SnapshotsConfig struct {
	ExpectBlocks uint64
}

func KnownSnapshots(networkName string) *SnapshotsConfig {
	switch networkName {
	case MainnetChainName:
		return MainnetChainSnapshotConfig
	case GoerliChainName:
		return GoerliChainSnapshotConfig
	default:
		return nil
	}
}
