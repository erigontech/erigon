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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func TestCaplinCheckManifestReportsIssues(t *testing.T) {
	manifest := writeCaplinCheckManifest(t, []string{
		snaptype.BeaconBlocks.FileName(version.V1_1, 0, 10_000),
		snaptype.BeaconBlocks.FileName(version.V1_1, 9_000, 20_000),
		snaptype.BlobSidecars.FileName(version.V1_1, 0, 10_000),
		snaptype.BlobSidecars.FileName(version.V1_1, 20_000, 30_000),
		snaptype.StateRoot.FileName(version.V1_1, 0, 100_000),
		snaptype.StateEvents.FileName(version.V1_1, 0, 100_000),
		snaptype.EpochData.FileName(version.V1_1, 0, 90_000),
		"caplin/v1.1-000000-000100-BlockProposers.seg",
	})

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	err := app.Run(t.Context(), []string{"capcli", "seg", "check-manifest", manifest})
	require.Error(t, err)
	require.Equal(t, "manifest "+manifest+"\n"+
		"family        table      files largest span\n"+
		"beacon_blocks -          2     "+snaptype.BeaconBlocks.FileName(version.V1_1, 9_000, 20_000)+" 11000\n"+
		"blob_sidecars -          2     "+snaptype.BlobSidecars.FileName(version.V1_1, 0, 10_000)+" 10000\n"+
		"beacon_state  EpochData  1     "+snaptype.EpochData.FileName(version.V1_1, 0, 90_000)+" 90000\n"+
		"beacon_state  StateEvents 1     "+snaptype.StateEvents.FileName(version.V1_1, 0, 100_000)+" 100000\n"+
		"beacon_state  StateRoot  1     "+snaptype.StateRoot.FileName(version.V1_1, 0, 100_000)+" 100000\n"+
		"gaps\n"+
		"  blob_sidecars 10000..20000\n"+
		"overlaps\n"+
		"  beacon_blocks 9000..10000\n"+
		"state table differences\n"+
		"  EpochData end 90000, common end 100000; boundaries 0..90000, common 0..100000\n"+
		"unregistered\n"+
		"  caplin/v1.1-000000-000100-BlockProposers.seg\n"+
		"invalid\n", output.String())

	var jsonOutput bytes.Buffer
	jsonApp := &cli.Command{Name: "capcli", Commands: commands(), Writer: &jsonOutput, ErrWriter: &jsonOutput}
	err = jsonApp.Run(t.Context(), []string{"capcli", "seg", "check-manifest", manifest, "--json"})
	require.Error(t, err)
	var report map[string]any
	require.NoError(t, json.Unmarshal(jsonOutput.Bytes(), &report))
	require.Len(t, report["gaps"], 1)
	require.Len(t, report["overlaps"], 1)
	require.Len(t, report["unregistered"], 1)
	require.Len(t, report["state_table_differences"], 1)
}

func TestCaplinCheckManifestClean(t *testing.T) {
	manifest := writeCaplinCheckManifest(t, []string{
		snaptype.BeaconBlocks.FileName(version.V1_1, 0, 10_000),
		snaptype.BeaconBlocks.FileName(version.V1_1, 10_000, 20_000),
		snaptype.BlobSidecars.FileName(version.V1_1, 0, 10_000),
		snaptype.BlobSidecars.FileName(version.V1_1, 10_000, 20_000),
		snaptype.StateRoot.FileName(version.V1_1, 0, 10_000),
		snaptype.EpochData.FileName(version.V1_1, 0, 10_000),
	})

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "check-manifest", manifest}))
	require.Contains(t, output.String(), "valid\n")
}

func TestCaplinCheckManifestDifferentStateEndFails(t *testing.T) {
	manifest := writeCaplinCheckManifest(t, []string{
		snaptype.BeaconBlocks.FileName(version.V1_1, 0, 10_000),
		snaptype.BlobSidecars.FileName(version.V1_1, 0, 10_000),
		snaptype.StateRoot.FileName(version.V1_1, 0, 100_000),
		snaptype.StateEvents.FileName(version.V1_1, 0, 100_000),
		snaptype.EpochData.FileName(version.V1_1, 0, 90_000),
	})

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	err := app.Run(t.Context(), []string{"capcli", "seg", "check-manifest", manifest})
	require.Error(t, err)
	require.Contains(t, output.String(), "EpochData end 90000, common end 100000")
	require.Contains(t, output.String(), "invalid\n")
}

func writeCaplinCheckManifest(t *testing.T, names []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.toml")
	data := make([]byte, 0, len(names)*80)
	for i, name := range names {
		data = fmt.Appendf(data, "'%s' = '%040d'\n", name, i+1)
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func runManifestReview(t *testing.T, names ...string) (caplinManifestReport, error) {
	t.Helper()
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output}
	err := app.Run(t.Context(), []string{"capcli", "seg", "check-manifest", writeCaplinCheckManifest(t, names), "--json"})
	var report caplinManifestReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &report))
	return report, err
}

func TestManifestNestedCoverage(t *testing.T) {
	report, err := runManifestReview(t,
		snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100),
		snaptype.BeaconBlocks.FileName(version.V1_1, 20, 30),
		snaptype.BeaconBlocks.FileName(version.V1_1, 50, 125),
	)
	want := []caplinManifestRange{
		{Family: "beacon_blocks", From: 20, To: 30},
		{Family: "beacon_blocks", From: 50, To: 100},
	}
	if err != nil || len(report.Gaps) != 0 || !slices.Equal(report.Overlaps, want) {
		t.Fatalf("continuous blocks: gaps=%v overlaps=%v err=%v", report.Gaps, report.Overlaps, err)
	}
}

func TestManifestDuplicateBoundaries(t *testing.T) {
	report, err := runManifestReview(t,
		snaptype.StateEvents.FileName(version.V1_1, 0, 100),
		snaptype.StateRoot.FileName(version.V1_0, 0, 100),
		snaptype.StateRoot.FileName(version.V1_1, 0, 100),
	)
	require.NoError(t, err)
	require.Len(t, report.Overlaps, 1)
	if len(report.StateTableDifferences) != 0 {
		t.Fatalf("same boundaries: differences=%v", report.StateTableDifferences)
	}
}

func TestManifestCommonEnd(t *testing.T) {
	report, err := runManifestReview(t,
		snaptype.PtcWindowTable.FileName(version.V1_1, 0, 100),
		snaptype.StateRoot.FileName(version.V1_1, 0, 200),
		snaptype.StateEvents.FileName(version.V1_1, 0, 150),
		snaptype.StateEvents.FileName(version.V1_1, 150, 200),
	)
	require.Error(t, err)
	var shortTables []string
	for _, diff := range report.StateTableDifferences {
		if diff.End != diff.CommonEnd {
			shortTables = append(shortTables, fmt.Sprintf("%s=%d (common %d)", diff.Table, diff.End, diff.CommonEnd))
		}
	}
	if !slices.Equal(shortTables, []string{"PtcWindow=100 (common 200)"}) {
		t.Fatalf("end outliers=%v", shortTables)
	}
}

func TestManifestIndexOnlyTable(t *testing.T) {
	report, err := runManifestReview(t,
		snaptype.StateEvents.FileName(version.V1_1, 0, 100),
		snaptype.StateRoot.IdxFileName(version.V1_1, 0, 100),
	)
	require.Len(t, report.Groups, 2)
	if err == nil || len(report.StateTableDifferences) == 0 {
		t.Fatalf("StateRoot has no segment: end differences=%v err=%v", report.StateTableDifferences, err)
	}
}

func TestManifestTorrentCount(t *testing.T) {
	seg := snaptype.StateRoot.FileName(version.V1_1, 0, 100)
	report, err := runManifestReview(t, seg, snaptype.StateRoot.IdxFileName(version.V1_1, 0, 100), seg+".torrent")
	require.NoError(t, err)
	require.Len(t, report.Groups, 1)
	if got := report.Groups[0].Files; got != 2 {
		t.Fatalf("segment and index: want 2 snapshot files, got %d", got)
	}
}

func TestManifestReversedRange(t *testing.T) {
	report, _ := runManifestReview(t,
		snaptype.StateRoot.FileName(version.V1_1, 0, 100),
		"v1.1-000000110-000000100-StateRoot.seg",
	)
	require.Len(t, report.Groups, 1)
	if got := report.Groups[0].LargestSpan; got != 100 {
		t.Fatalf("largest valid span: want 100, got %d", got)
	}
}

func TestManifestRegisteredELType(t *testing.T) {
	const name = "v1.0-000000-000100-headers.seg"
	info, _, ok := snaptype.ParseFileName("", name)
	require.True(t, ok)
	require.NotNil(t, info.Type)
	report, err := runManifestReview(t, name, "bad-name", "caplin/v1.1-000000-000100-BlockProposers.seg")
	require.NoError(t, err)
	if slices.Contains(report.Unregistered, name) {
		t.Fatalf("registered type %s listed as unregistered", info.Type.Name())
	}
}
