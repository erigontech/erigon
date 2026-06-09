// Copyright 2026 The Erigon Authors
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

package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeVersion(t *testing.T) {
	origTag, origCommit := GitTag, GitCommit
	t.Cleanup(func() { GitTag, GitCommit = origTag, origCommit })

	const commit = "a53e954520442aa91a92f111eb23b213ffc800b7"

	// A build from an exact release tag reports that tag verbatim.
	GitTag, GitCommit = "v3.5.0", commit
	require.Equal(t, "v3.5.0", NodeVersion())

	// A dev build (no tag) reports the in-development version plus short commit.
	GitTag, GitCommit = "", commit
	require.Equal(t, VersionWithMeta+"-a53e9545", NodeVersion())

	// A dev build with no commit info falls back to the in-development version.
	GitTag, GitCommit = "", ""
	require.Equal(t, VersionWithMeta, NodeVersion())

	// Version-shaped values are honored verbatim: release tags, pre-releases,
	// and git-describe / operator-supplied (env) forms for builds without .git.
	GitCommit = commit
	for _, good := range []string{
		"v3.5.0", "v3.5.1", "v3.4.0-rc.1", "v3.0.0-beta1", "v3.5",
		"3.5.0", "v3.5.0-5-gabcdef", "v3.0.0-beta1-4014-ga53e954",
	} {
		GitTag = good
		require.Equal(t, good, NodeVersion(), "tag %q must be accepted", good)
	}

	// GitTag is untrusted build-time git metadata. Shell/command-injection
	// payloads that are nonetheless valid git ref names, and non-version junk,
	// must be rejected and never advertised.
	for _, bad := range []string{
		"garbage", "v3.5.0 -X evil",
		"v3.5.0`id`", "v3.5.0$(whoami)", "v3.5.0;rm -rf /", "v3.5.0|cat",
	} {
		GitTag = bad
		require.Equal(t, VersionWithMeta+"-a53e9545", NodeVersion(), "tag %q must be rejected", bad)
	}
}
