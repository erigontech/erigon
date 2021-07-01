/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package gointerfaces

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
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
