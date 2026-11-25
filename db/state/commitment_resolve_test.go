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

package state

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestResolveCommitmentV3(t *testing.T) {
	write := func(t *testing.T, dir, name string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o644))
	}
	for _, test := range []struct {
		name       string
		pin        *bool
		files      func(dirs datadir.Dirs)
		wantLegacy bool
		wantV3     bool
	}{
		{name: "fresh datadir keeps the v3 default", files: func(datadir.Dirs) {}, wantV3: true},
		{name: "legacy domain file switches to legacy", files: func(d datadir.Dirs) { write(t, d.SnapDomain, "v2.0-commitment.0-1.kv") }, wantLegacy: true},
		{name: "legacy history file switches to legacy", files: func(d datadir.Dirs) { write(t, d.SnapHistory, "v2.0-commitment.0-1.v") }, wantLegacy: true},
		{name: "v3 domain file keeps v3", files: func(d datadir.Dirs) { write(t, d.SnapDomain, "v3.0-commitment.0-1.kv") }, wantV3: true},
		{name: "mixed files keep v3 for the mixed-files check", files: func(d datadir.Dirs) {
			write(t, d.SnapDomain, "v3.0-commitment.0-1.kv")
			write(t, d.SnapHistory, "v2.0-commitment.0-1.v")
		}, wantV3: true},
		{name: "legacy accounts file alone keeps v3", files: func(d datadir.Dirs) { write(t, d.SnapDomain, "v2.0-accounts.0-1.kv") }, wantV3: true},
		{name: "pinned v3 is not switched by legacy files", pin: new(true), files: func(d datadir.Dirs) { write(t, d.SnapDomain, "v2.0-commitment.0-1.kv") }, wantV3: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(statecfg.ResetCommitmentV3())
			if test.pin != nil {
				statecfg.SetCommitmentV3(*test.pin)
			}
			dirs := datadir.New(t.TempDir())
			test.files(dirs)

			legacyFile, err := ResolveCommitmentV3(dirs)
			require.NoError(t, err)
			require.Equal(t, test.wantLegacy, legacyFile != "", legacyFile)
			require.Equal(t, test.wantV3, statecfg.ExperimentalCommitmentV3)
			require.Equal(t, test.wantV3, statecfg.Schema.CommitmentDomain.CommitmentV3Records)
			require.Equal(t, test.pin != nil || test.wantLegacy, statecfg.CommitmentV3Pinned())
		})
	}
}
