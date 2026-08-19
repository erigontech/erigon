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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

// reportSnapDomain writes files of the given byte lengths and returns the
// directory holding them.
func reportSnapDomain(t *testing.T, sizes map[string]int) string {
	t.Helper()
	snapDomain := t.TempDir()
	for name, size := range sizes {
		require.NoError(t, os.WriteFile(filepath.Join(snapDomain, name), make([]byte, size), 0o644))
	}
	return snapDomain
}

func TestCommitmentFileSizesMatchDisk(t *testing.T) {
	snapDomain := reportSnapDomain(t, map[string]int{
		"v1.0-commitment.0-64.kv":    4096,
		"v1.0-commitment.64-128.kv":  512,
		"v1.0-commitment.128-256.kv": 256,
		"v1.0-commitment.0-64.kvi":   99,   // an accessor is not the file the rebuild sized
		"v1.0-accounts.0-64.kv":      8192, // another domain's data, hardlinked into the output
	})

	files, err := commitmentFileSizes(snapDomain)
	require.NoError(t, err)
	require.Len(t, files, 3, "only commitment .kv files carry commitment data")

	// Step order, not name order: 128-256 sorts before 64-128 by name.
	require.Equal(t, "v1.0-commitment.0-64.kv", files[0].Name)
	require.Equal(t, uint64(0), files[0].StepFrom)
	require.Equal(t, uint64(64), files[0].StepTo)
	require.Equal(t, "v1.0-commitment.64-128.kv", files[1].Name)
	require.Equal(t, uint64(64), files[1].StepFrom)
	require.Equal(t, uint64(128), files[1].StepTo)
	require.Equal(t, "v1.0-commitment.128-256.kv", files[2].Name)
	require.Equal(t, uint64(128), files[2].StepFrom)
	require.Equal(t, uint64(256), files[2].StepTo)

	var total int64
	for _, f := range files {
		info, err := os.Stat(filepath.Join(snapDomain, f.Name))
		require.NoError(t, err)
		require.Equal(t, info.Size(), f.Bytes, "%s must report the bytes it takes on disk", f.Name)
		total += info.Size()
	}
	require.Equal(t, int64(4864), total)
	require.Equal(t, total, totalCommitmentBytes(files))
}

func TestCommitmentFileSizesMissingDir(t *testing.T) {
	files, err := commitmentFileSizes(filepath.Join(t.TempDir(), "never-written"))
	require.NoError(t, err, "a rebuild that produced nothing reports nothing, it does not fail")
	require.Empty(t, files)
}

func TestFormatRebuildReport(t *testing.T) {
	snapDomain := reportSnapDomain(t, map[string]int{
		"v1.0-commitment.0-64.kv":   4096,
		"v1.0-commitment.64-128.kv": 512,
	})
	files, err := commitmentFileSizes(snapDomain)
	require.NoError(t, err)

	report := &dbstate.RebuildReport{
		Target: dbstate.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashBlake3},
		Ranges: []dbstate.RebuildRangeReport{{
			StepFrom:      0,
			StepTo:        64,
			TxnFrom:       0,
			TxnTo:         6400,
			KeysInFiles:   900,
			KeysProcessed: 900,
			RootHash:      []byte{0xab, 0xcd},
			Shards: []dbstate.RebuildShardReport{
				{StepFrom: 0, StepTo: 32, Keys: 500, CodeBearingAccounts: 40, UniqueCodeHashes: 12},
				{StepFrom: 32, StepTo: 64, Keys: 400, CodeBearingAccounts: 30, UniqueCodeHashes: 9},
			},
		}},
	}

	out := formatRebuildReport(files, report)
	for _, field := range []string{
		"file\tstep_from\tstep_to\tbytes",
		"step_from\tstep_to\ttxn_from\ttxn_to\tkeys_in_files\tkeys_processed\troot",
		"range_step_from\trange_step_to\tstep_from\tstep_to\tkeys\tcode_accounts\tunique_code_hashes",
	} {
		require.Contains(t, out, field, "the report's column names are what makes it pasteable")
	}

	require.Contains(t, out, "v1.0-commitment.0-64.kv\t0\t64\t4096")
	require.Contains(t, out, "v1.0-commitment.64-128.kv\t64\t128\t512")
	require.Contains(t, out, "total\t0\t128\t4608")
	require.Contains(t, out, "0\t64\t0\t6400\t900\t900\tabcd")
	require.Contains(t, out, "0\t64\t0\t32\t500\t40\t12")
	require.Contains(t, out, "0\t64\t32\t64\t400\t30\t9")
	require.Contains(t, out, string(commitment.VariantBinPatriciaTrie))
	require.Contains(t, out, commitment.PBinHashBlake3)
}

func TestFormatRebuildReportWithoutCounts(t *testing.T) {
	files, err := commitmentFileSizes(reportSnapDomain(t, map[string]int{"v1.0-commitment.0-64.kv": 128}))
	require.NoError(t, err)

	out := formatRebuildReport(files, nil)
	require.Contains(t, out, "v1.0-commitment.0-64.kv\t0\t64\t128")
	require.Contains(t, out, "total\t0\t64\t128")
	require.NotContains(t, out, "keys_processed")
}

// The report describes what the rebuild wrote, so a run with --output.datadir
// must size the output directory rather than the source it read.
func TestRebuildReportDirIsTheOutput(t *testing.T) {
	src := sourceDatadirFixture(t)
	out, err := stageRebuildOutput(src, filepath.Join(t.TempDir(), "out"), binTarget(t), false, log.New())
	require.NoError(t, err)

	require.Equal(t, out.dirs.SnapDomain, rebuildReportDir(out, src))
	require.Equal(t, src.SnapDomain, rebuildReportDir(nil, src))

	files, err := commitmentFileSizes(rebuildReportDir(out, src))
	require.NoError(t, err)
	require.Empty(t, files, "the staged output holds no commitment files until the rebuild writes them")

	require.NoError(t, os.WriteFile(filepath.Join(out.dirs.SnapDomain, "v1.0-commitment.0-64.kv"), make([]byte, 77), 0o644))
	files, err = commitmentFileSizes(rebuildReportDir(out, src))
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, int64(77), files[0].Bytes)

	srcFiles, err := commitmentFileSizes(src.SnapDomain)
	require.NoError(t, err)
	require.Len(t, srcFiles, 1)
	require.NotEqual(t, files[0].Bytes, srcFiles[0].Bytes, "the source's own commitment file is a different file")
	require.Contains(t, srcFiles[0].Name, kv.CommitmentDomain.String())
}
