// Copyright 2021 The Erigon Authors
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

package gointerfaces

import (
	"fmt"

	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

type Version struct {
	Major, Minor, Patch uint32 // interface Version of the client - to perform compatibility check when opening
}

func VersionFromProto(r *types.VersionReply) Version {
	return Version{Major: r.Major, Minor: r.Minor, Patch: r.Patch}
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
