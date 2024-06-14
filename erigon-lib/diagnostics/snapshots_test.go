package diagnostics_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
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
