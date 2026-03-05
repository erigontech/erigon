package diaglib_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/diaglib"
)

func TestParseData(t *testing.T) {
	var data []byte
	var v diaglib.RAMInfo
	diaglib.ParseData(data, v)
	require.Equal(t, diaglib.RAMInfo{}, v)

	newv := diaglib.RAMInfo{
		Total:       1,
		Available:   2,
		Used:        3,
		UsedPercent: 4,
	}

	data, err := json.Marshal(newv)
	require.NoError(t, err)

	diaglib.ParseData(data, &v)
	require.Equal(t, newv, v)
}

// Testing the function CalculateSyncStageStats
func TestCalculateSyncStageStats(t *testing.T) {
	sds := diaglib.SnapshotDownloadStatistics{
		Downloaded:           100,
		Total:                200,
		TorrentMetadataReady: 10,
		Files:                10,
		DownloadRate:         10,
		TotalTime:            1000,
	}

	expected := diaglib.SyncStageStats{
		TimeElapsed: "16m40s",
		TimeLeft:    "10s",
		Progress:    "50%",
	}

	require.Equal(t, expected, diaglib.CalculateSyncStageStats(sds))
}

// Test CalculateTime function
func TestCalculateTime(t *testing.T) {
	require.Equal(t, "999h:99m", diaglib.CalculateTime(0, 0))
	require.Equal(t, "999h:99m", diaglib.CalculateTime(1, 0))
	require.Equal(t, "1s", diaglib.CalculateTime(1, 1))
	require.Equal(t, "10s", diaglib.CalculateTime(10, 1))
	require.Equal(t, "2m:40s", diaglib.CalculateTime(160, 1))
	require.Equal(t, "1h:40m", diaglib.CalculateTime(6000, 1))
}

// Test GetShanpshotsPercentDownloaded function
func TestGetShanpshotsPercentDownloaded(t *testing.T) {
	require.Equal(t, "0%", diaglib.GetShanpshotsPercentDownloaded(0, 0, 0, 0))
	require.Equal(t, "0%", diaglib.GetShanpshotsPercentDownloaded(0, 1, 0, 0))
	require.Equal(t, "100%", diaglib.GetShanpshotsPercentDownloaded(1, 1, 1, 1))
	require.Equal(t, "50%", diaglib.GetShanpshotsPercentDownloaded(1, 2, 1, 1))

	require.Equal(t, "50.01%", diaglib.GetShanpshotsPercentDownloaded(5001, 10000, 1, 1))
	require.Equal(t, "50.5%", diaglib.GetShanpshotsPercentDownloaded(5050, 10000, 1, 1))

	require.Equal(t, "calculating...", diaglib.GetShanpshotsPercentDownloaded(10000, 10000, 0, 1))
}
