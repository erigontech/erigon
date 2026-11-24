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
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/debug"
)

func TestCommitmentVisualizeReadsLegacyFiles(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	_, dirs, _ := dbWithState(t, true)
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	_, err = processCommitmentFile(files[0])
	require.NoError(t, err)
}

// A file whose name has no version is decoded, and a record that is no legacy branch
// fails the file instead of panicking.
func TestCommitmentVisualizeFailsOnAnUnnamedV3File(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	_, dirs, _ := dbWithState(t, true)
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "v3.*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	unnamed := filepath.Join(t.TempDir(), "commitment.kv")
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(unnamed, data, 0o600))
	_, err = processCommitmentFile(unnamed)
	require.ErrorContains(t, err, "failed to decode branch")
}

// A file that cannot be charted fails the command with no usage text, no empty page
// is written, and profiling stops.
func TestCommitmentVisualizeCommandFailsOnAV3File(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	_, dirs, _ := dbWithState(t, true)
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "v3.*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	out := t.TempDir()
	prevOutput := visualizeOutputDir
	t.Cleanup(func() { visualizeOutputDir = prevOutput })
	root := integrationRoot()
	cmdCommitmentVisualize.SetContext(t.Context())
	root.SetArgs([]string{"--log.dir.disable", "--pprof.cpuprofile", filepath.Join(t.TempDir(), "cpu.out"), "commitment", "visualize", "--output", out, files[0]})
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	t.Cleanup(func() {
		root.SetOut(nil)
		root.SetErr(nil)
		cpu := root.PersistentFlags().Lookup("pprof.cpuprofile")
		_ = cpu.Value.Set("")
		cpu.Changed = false
	})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "v3 commitment records")
	require.NotContains(t, output.String(), "Usage:")
	require.ErrorContains(t, debug.Handler.StopCPUProfile(), "not in progress")
	require.NoFileExists(t, filepath.Join(out, "analysis.html"))
}
