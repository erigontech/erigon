// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

import (
	"fmt"
)

// see https://calver.org
const (
	VersionMajor    = 2020    // Major version component of the current release
	VersionMinor    = 10      // Minor version component of the current release
	VersionMicro    = 1       // Patch version component of the current release
	VersionModifier = "alpha" // Patch version component of the current release
)

func withModifier(vsn string) string {
	if !isStable() {
		vsn += "-" + VersionModifier
	}
	return vsn
}

func isStable() bool {
	return VersionModifier == "stable"
}

func isRelease() bool {
	return isStable() || VersionModifier == "alpha" || VersionModifier == "beta"
}

// Version holds the textual version string.
var Version = func() string {
	return fmt.Sprintf("%d.%02d.%d", VersionMajor, VersionMinor, VersionMicro)
}()

// VersionWithMeta holds the textual version string including the metadata.
var VersionWithMeta = func() string {
	v := Version
	if VersionModifier != "" {
		v += "-" + VersionModifier
	}
	return v
}()

// ArchiveVersion holds the textual version string used for Geth archives.
// e.g. "1.8.11-dea1ce05" for stable releases, or
//      "1.8.13-unstable-21c059b6" for unstable releases
func ArchiveVersion(gitCommit string) string {
	vsn := withModifier(Version)
	if len(gitCommit) >= 8 {
		vsn += "-" + gitCommit[:8]
	}
	return vsn
}

func VersionWithCommit(gitCommit, gitDate string) string {
	vsn := VersionWithMeta
	if len(gitCommit) >= 8 {
		vsn += "-" + gitCommit[:8]
	}
	if !isRelease() && (gitDate != "") {
		vsn += "-" + gitDate
	}
	return vsn
}
