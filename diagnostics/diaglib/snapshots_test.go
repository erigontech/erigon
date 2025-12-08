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

package diaglib_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/diaglib"
)

func NewTestDiagnosticClient() (*diaglib.DiagnosticClient, error) {
	return &diaglib.DiagnosticClient{}, nil
}

func TestUpdateFileDownloadingStats(t *testing.T) {
	d, err := NewTestDiagnosticClient()

	require.NoError(t, err)

	d.UpdateFileDownloadedStatistics(nil, &segmentDownloadStatsMock)

	sd := d.SyncStatistics().SnapshotDownload.SegmentsDownloading
	require.NotNil(t, sd)
	require.NotEmpty(t, sd)

	require.Equal(t, sd["test"], segmentDownloadStatsMock)

	d.UpdateFileDownloadedStatistics(&fileDownloadedUpdMock, nil)

	sd = d.SyncStatistics().SnapshotDownload.SegmentsDownloading

	toccompare := diaglib.SegmentDownloadStatistics{
		Name:            "test",
		TotalBytes:      1,
		DownloadedBytes: 1,
		Webseeds:        make([]diaglib.SegmentPeer, 0),
		Peers:           make([]diaglib.SegmentPeer, 0),
		DownloadedStats: diaglib.FileDownloadedStatistics{
			TimeTook:    1.0,
			AverageRate: 1,
		},
	}
	require.Equal(t, sd["test"], toccompare)
}

var (
	fileDownloadedUpdMock = diaglib.FileDownloadedStatisticsUpdate{
		FileName:    "test",
		TimeTook:    1.0,
		AverageRate: 1,
	}

	segmentDownloadStatsMock = diaglib.SegmentDownloadStatistics{
		Name:            "test",
		TotalBytes:      1,
		DownloadedBytes: 1,
		Webseeds:        make([]diaglib.SegmentPeer, 0),
		Peers:           make([]diaglib.SegmentPeer, 0),
		DownloadedStats: diaglib.FileDownloadedStatistics{},
	}
)

func TestPercentDiownloaded(t *testing.T) {
	downloaded := uint64(10)
	total := uint64(100)
	files := int32(20)
	torrentMetadataReady := int32(10)

	//Test metadata not ready
	progress := diaglib.GetShanpshotsPercentDownloaded(downloaded, total, torrentMetadataReady, files)
	require.Equal(t, "calculating...", progress)

	//Test metadata ready
	progress = diaglib.GetShanpshotsPercentDownloaded(downloaded, total, files, files)
	require.Equal(t, "10%", progress)

	//Test 100 %
	progress = diaglib.GetShanpshotsPercentDownloaded(total, total, files, files)
	require.Equal(t, "100%", progress)

	//Test 0 %
	progress = diaglib.GetShanpshotsPercentDownloaded(0, total, files, files)
	require.Equal(t, "0%", progress)

	//Test more than 100 %
	progress = diaglib.GetShanpshotsPercentDownloaded(total+1, total, files, files)
	require.Equal(t, "100%", progress)
}

func TestFillDBFromSnapshots(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	d.SetFillDBInfo(diaglib.SnapshotFillDBStage{StageName: "Headers", Current: 1, Total: 10})
	stats := d.SyncStatistics()
	require.NotEmpty(t, stats.SnapshotFillDB.Stages)
	require.Equal(t, diaglib.SnapshotFillDBStage{StageName: "Headers", Current: 1, Total: 10}, stats.SnapshotFillDB.Stages[0])
}

func TestAddOrUpdateSegmentIndexingState(t *testing.T) {
	dts := []diaglib.SnapshotSegmentIndexingStatistics{
		{
			SegmentName: "test",
			Percent:     50,
			Alloc:       0,
			Sys:         0,
		},
	}

	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	d.AddOrUpdateSegmentIndexingState(diaglib.SnapshotIndexingStatistics{
		Segments:    dts,
		TimeElapsed: -1,
	})
	stats := d.SyncStatistics()

	require.NotEmpty(t, stats.SnapshotIndexing)
	require.NotEmpty(t, stats.SnapshotIndexing.Segments)
	require.Equal(t, stats.SnapshotIndexing.Segments[0], dts[0])
	require.Zero(t, stats.SnapshotIndexing.TimeElapsed)
	require.False(t, stats.SnapshotIndexing.IndexingFinished)

	dts = []diaglib.SnapshotSegmentIndexingStatistics{
		{
			SegmentName: "test",
			Percent:     100,
			Alloc:       0,
			Sys:         0,
		},
		{
			SegmentName: "test2",
			Percent:     10,
			Alloc:       0,
			Sys:         0,
		},
	}

	d.AddOrUpdateSegmentIndexingState(diaglib.SnapshotIndexingStatistics{
		Segments:    dts,
		TimeElapsed: 20,
	})

	stats = d.SyncStatistics()
	require.Equal(t, 100, stats.SnapshotIndexing.Segments[0].Percent)

	finished := d.UpdateIndexingStatus()
	require.False(t, finished)

	//test indexing finished
	dts = []diaglib.SnapshotSegmentIndexingStatistics{
		{
			SegmentName: "test2",
			Percent:     100,
			Alloc:       0,
			Sys:         0,
		},
	}
	d.AddOrUpdateSegmentIndexingState(diaglib.SnapshotIndexingStatistics{
		Segments:    dts,
		TimeElapsed: 20,
	})

	finished = d.UpdateIndexingStatus()
	require.True(t, finished)
	stats = d.SyncStatistics()
	require.True(t, stats.SnapshotIndexing.IndexingFinished)
}
