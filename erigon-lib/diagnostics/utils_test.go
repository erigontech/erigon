package diagnostics_test

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/stretchr/testify/require"
)

func TestParseData(t *testing.T) {
	var data []byte
	var v diagnostics.RAMInfo
	diagnostics.ParseData(data, v)
	require.Equal(t, diagnostics.RAMInfo{}, v)

	newv := diagnostics.RAMInfo{
		Total:       1,
		Available:   2,
		Used:        3,
		UsedPercent: 4,
	}

	data, err := json.Marshal(newv)
	require.NoError(t, err)

	diagnostics.ParseData(data, &v)
	require.Equal(t, newv, v)
}

// Testing the function CalculateSyncStageStats
func TestCalculateSyncStageStats(t *testing.T) {
	sds := diagnostics.SnapshotDownloadStatistics{
		Downloaded:           100,
		Total:                200,
		TorrentMetadataReady: 10,
		Files:                10,
		DownloadRate:         10,
		TotalTime:            1000,
	}

	expected := diagnostics.SyncStageStats{
		TimeElapsed: "16m40s",
		TimeLeft:    "10s",
		Progress:    "50%",
	}

	require.Equal(t, expected, diagnostics.CalculateSyncStageStats(sds))
}

// Test CalculateTime function
func TestCalculateTime(t *testing.T) {
	require.Equal(t, "999h:99m", diagnostics.CalculateTime(0, 0))
	require.Equal(t, "999h:99m", diagnostics.CalculateTime(1, 0))
	require.Equal(t, "1s", diagnostics.CalculateTime(1, 1))
	require.Equal(t, "10s", diagnostics.CalculateTime(10, 1))
	require.Equal(t, "2m:40s", diagnostics.CalculateTime(160, 1))
	require.Equal(t, "1h:40m", diagnostics.CalculateTime(6000, 1))
}

// Test GetShanpshotsPercentDownloaded function
func TestGetShanpshotsPercentDownloaded(t *testing.T) {
	require.Equal(t, "0%", diagnostics.GetShanpshotsPercentDownloaded(0, 0, 0, 0))
	require.Equal(t, "0%", diagnostics.GetShanpshotsPercentDownloaded(0, 1, 0, 0))
	require.Equal(t, "100%", diagnostics.GetShanpshotsPercentDownloaded(1, 1, 1, 1))
	require.Equal(t, "50%", diagnostics.GetShanpshotsPercentDownloaded(1, 2, 1, 1))

	require.Equal(t, "50.01%", diagnostics.GetShanpshotsPercentDownloaded(5001, 10000, 1, 1))
	require.Equal(t, "50.5%", diagnostics.GetShanpshotsPercentDownloaded(5050, 10000, 1, 1))

	require.Equal(t, "calculating...", diagnostics.GetShanpshotsPercentDownloaded(10000, 10000, 0, 1))
}
