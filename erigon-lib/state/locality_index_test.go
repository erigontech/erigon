package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanStaticFilesLocality(t *testing.T) {

	t.Run("new", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.enableLocalityIndex()
		files := []string{
			"test.0-1.l",
			"test.1-2.l",
			"test.0-4.l",
			"test.2-3.l",
			"test.3-4.l",
			"test.4-5.l",
		}
		ii.warmLocalityIdx.scanStateFiles(files)
		require.Equal(t, 4, int(ii.warmLocalityIdx.file.startTxNum))
		require.Equal(t, 5, int(ii.warmLocalityIdx.file.endTxNum))
		ii.coldLocalityIdx.scanStateFiles(files)
		require.Equal(t, 4, int(ii.coldLocalityIdx.file.startTxNum))
		require.Equal(t, 5, int(ii.coldLocalityIdx.file.endTxNum))
	})
	t.Run("overlap", func(t *testing.T) {
		ii := emptyTestInvertedIndex(1)
		ii.enableLocalityIndex()
		ii.warmLocalityIdx.scanStateFiles([]string{
			"test.0-50.l",
			"test.0-70.l",
			"test.64-70.l",
		})
		require.Equal(t, 64, int(ii.warmLocalityIdx.file.startTxNum))
		require.Equal(t, 70, int(ii.warmLocalityIdx.file.endTxNum))
		ii.coldLocalityIdx.scanStateFiles([]string{
			"test.0-32.l",
			"test.0-64.l",
		})
		require.Equal(t, 0, int(ii.coldLocalityIdx.file.startTxNum))
		require.Equal(t, 64, int(ii.coldLocalityIdx.file.endTxNum))
	})
}
