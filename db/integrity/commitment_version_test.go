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

package integrity

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
)

type fakeVisibleFile struct {
	startTxNum, endTxNum uint64
	ver                  version.Version
}

func (f fakeVisibleFile) Fullpath() string         { return "fake.kv" }
func (f fakeVisibleFile) StartRootNum() uint64     { return f.startTxNum }
func (f fakeVisibleFile) EndRootNum() uint64       { return f.endTxNum }
func (f fakeVisibleFile) Version() version.Version { return f.ver }

// TestCommitmentFileReferencing pins the integrity decision boundary: a commitment file
// carries shortened references only when its own version is below v2.1 AND its range reaches
// the referencing threshold. A v2.1 (plain) file must never be treated as referencing, even
// when its range is large.
func TestCommitmentFileReferencing(t *testing.T) {
	const stepSize = uint64(10)
	cases := []struct {
		name     string
		from, to uint64
		ver      version.Version
		want     bool
	}{
		{"v2.0 at threshold is referencing", 0, 2 * stepSize, version.V2_0, true},
		{"v1.0 at threshold is referencing", 0, 2 * stepSize, version.V1_0, true},
		{"v2.1 is always plain", 0, 2 * stepSize, version.V2_1, false},
		{"v2.0 below threshold is plain", 0, stepSize, version.V2_0, false},
		{"hypothetical v2.2 is plain", 0, 2 * stepSize, version.Version{Major: 2, Minor: 2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeVisibleFile{startTxNum: tc.from, endTxNum: tc.to, ver: tc.ver}
			require.Equal(t, tc.want, commitmentFileReferencing(f, stepSize))
		})
	}
}
