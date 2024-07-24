// Copyright 2024 The Erigon Authors
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

package diagnostics_test

import (
	"testing"

	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/stretchr/testify/require"
)

func NewTestDiagnosticClient() (*diagnostics.DiagnosticClient, error) {
	return &diagnostics.DiagnosticClient{}, nil
}

func TestUpdateFileDownloadingStats(t *testing.T) {
	d, err := NewTestDiagnosticClient()

	require.NoError(t, err)

	d.UpdateFileDownloadedStatistics(nil, &segmentDownloadStatsMock)

	sd := d.SyncStatistics().SnapshotDownload.SegmentsDownloading
	require.NotNil(t, sd)
	require.NotEqual(t, len(sd), 0)

	require.Equal(t, sd["test"], segmentDownloadStatsMock)

	d.UpdateFileDownloadedStatistics(&fileDownloadedUpdMock, nil)

	require.Equal(t, sd["test"], diagnostics.SegmentDownloadStatistics{
		Name:            "test",
		TotalBytes:      1,
		DownloadedBytes: 1,
		Webseeds:        make([]diagnostics.SegmentPeer, 0),
		Peers:           make([]diagnostics.SegmentPeer, 0),
		DownloadedStats: diagnostics.FileDownloadedStatistics{
			TimeTook:    1.0,
			AverageRate: 1,
		},
	})
}

var (
	fileDownloadedUpdMock = diagnostics.FileDownloadedStatisticsUpdate{
		FileName:    "test",
		TimeTook:    1.0,
		AverageRate: 1,
	}

	segmentDownloadStatsMock = diagnostics.SegmentDownloadStatistics{
		Name:            "test",
		TotalBytes:      1,
		DownloadedBytes: 1,
		Webseeds:        make([]diagnostics.SegmentPeer, 0),
		Peers:           make([]diagnostics.SegmentPeer, 0),
		DownloadedStats: diagnostics.FileDownloadedStatistics{},
	}
)

func TestPercentDiownloaded(t *testing.T) {
	downloaded := uint64(10)
	total := uint64(100)
	files := int32(20)
	torrentMetadataReady := int32(10)

	//Test metadata not ready
	progress := diagnostics.GetShanpshotsPercentDownloaded(downloaded, total, torrentMetadataReady, files)
	require.Equal(t, progress, "calculating...")

	//Test metadata ready
	progress = diagnostics.GetShanpshotsPercentDownloaded(downloaded, total, files, files)
	require.Equal(t, progress, "10.00%")

	//Test 100 %
	progress = diagnostics.GetShanpshotsPercentDownloaded(total, total, files, files)
	require.Equal(t, progress, "100.00%")

	//Test 0 %
	progress = diagnostics.GetShanpshotsPercentDownloaded(0, total, files, files)
	require.Equal(t, progress, "0.00%")

	//Test more than 100 %
	progress = diagnostics.GetShanpshotsPercentDownloaded(total+1, total, files, files)
	require.Equal(t, progress, "100.00%")
}

func TestFillDBFromSnapshots(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	d.SetFillDBInfo(diagnostics.SnapshotFillDBStage{StageName: "Headers", Current: 1, Total: 10})
	stats := d.SyncStatistics()
	require.NotEmpty(t, stats.SnapshotFillDB.Stages)
	require.Equal(t, stats.SnapshotFillDB.Stages[0], diagnostics.SnapshotFillDBStage{StageName: "Headers", Current: 1, Total: 10})
}
