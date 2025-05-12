package version

// see https://calver.org
const (
	Major                    = 3      // Major version component of the current release
	Minor                    = 1      // Minor version component of the current release
	Micro                    = 0      // Patch version component of the current release
	Modifier                 = "dev"  // Modifier component of the current release
	DefaultSnapshotGitBranch = "main" // Branch of erigontech/erigon-snapshot to use in OtterSync
)
