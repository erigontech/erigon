package gointerfaces

import (
	"fmt"

	"github.com/ledgerwatch/erigon/gointerfaces/types"
)

type Version struct {
	Major, Minor, Patch uint32 // interface Version of the client - to perform compatibility check when opening
}

// EnsureVersion - Default policy: allow only patch difference
func EnsureVersion(local Version, remote *types.VersionReply) bool {
	if remote.Major != local.Major {
		return false
	}
	if remote.Minor != local.Minor {
		return false
	}
	return true
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
