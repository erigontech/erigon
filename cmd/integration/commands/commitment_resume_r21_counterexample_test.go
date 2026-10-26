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

package commands

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
)

func TestPBTR21StraddleRefusalPreservesOutput(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 1, false, false)
	dirs := datadir.Open(output)
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	agg, err := openPBTState(t.Context(), dirs, settings, nil, log.New())
	require.NoError(t, err)
	require.NoError(t, agg.MergePBinRange(t.Context(), kv.CommitmentBinDomain, 1, 4))
	agg.Close()
	manifestPath := filepath.Join(dirs.Snap, "pbt-manifest.toml")
	manifest, err := dbstate.ReadPBTManifest(manifestPath)
	require.NoError(t, err)
	files, err := pbtAttachManifestSources(dirs)
	require.NoError(t, err)
	manifest, err = dbstate.BuildPBTManifest(t.Context(), manifest.Point, manifest.PBTRoot, manifest.HexRoot, manifest.TrieHash, manifest.SkipVerify, files, 1, nil)
	require.NoError(t, err)
	require.NoError(t, dbstate.WritePBTManifest(manifestPath, manifest))
	before := snapshotTreeDigest(t, output)
	err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "base bin file range 1-4 straddles source commitment ranges 1-2 and 2-3")
	after := snapshotTreeDigest(t, output)
	var changed int
	for name, digest := range before {
		if after[name] != digest {
			changed++
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			changed++
		}
	}
	require.Zero(t, changed, "straddle refusal must happen before any output change")
}

func TestPBTR21ZeroStartSpanningRangeMatchesFresh(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 1, false, false)
	sourceDirs := source.Tester.Dirs
	settings, err := dbstate.ReadErigonDBSettings(sourceDirs)
	require.NoError(t, err)
	baseSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	basePoint, err := readPBinSourcePoint(t.Context(), datadir.Open(output), baseSettings, false, log.New())
	require.NoError(t, err)
	t.Logf("X=%d", basePoint.TxNum)
	fresh := filepath.Join(t.TempDir(), "fresh")
	require.NoError(t, convertPBT(t.Context(), sourceDirs.DataDir, fresh, true, "", log.New()))
	agg, err := openPBTState(t.Context(), sourceDirs, settings, nil, log.New())
	require.NoError(t, err)
	require.NoError(t, agg.MergePBinRange(t.Context(), kv.CommitmentDomain, 0, basePoint.TxNum+3))
	agg.Close()
	err = convertPBTWithOptions(t.Context(), sourceDirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.NoError(t, err, "resume with nested base files must produce the spanning source range")
	require.Equal(t, readPBTBinRows(t, fresh, sourceDirs.Chaindata), readPBTBinRows(t, output, sourceDirs.Chaindata))
	require.Equal(t, readPBTBinRoot(t, fresh, sourceDirs.Chaindata), readPBTBinRoot(t, output, sourceDirs.Chaindata))
	require.Equal(t, pbtResumeRangeKeys(t, sourceDirs, kv.CommitmentDomain), pbtResumeRangeKeys(t, datadir.Open(output), kv.CommitmentBinDomain))
	setExecutionProgress(t, sourceDirs.Chaindata, 1)
	require.NoError(t, attachPBTWithHooks(t.Context(), sourceDirs.DataDir, output, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
}
