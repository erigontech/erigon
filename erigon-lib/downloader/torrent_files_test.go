package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/stretchr/testify/require"
)

func TestFSProhibitBackwardCompat(t *testing.T) {
	require := require.New(t)
	dirs := datadir.New(t.TempDir())

	//prev version of .lock - is empty .lock file which exitence prohibiting everything
	t.Run("no prev version .lock", func(t *testing.T) {
		tf := NewAtomicTorrentFiles(dirs.Snap)
		prohibited, err := tf.NewDownloadsAreProhibited("v1-004900-005000-headers.seg")
		require.NoError(err)
		require.False(prohibited)
		prohibited, err = tf.NewDownloadsAreProhibited("v1-004900-005000-headers.seg.torrent")
		require.NoError(err)
		require.False(prohibited)
	})
	t.Run("prev version .lock support", func(t *testing.T) {
		err := os.WriteFile(filepath.Join(dirs.Snap, ProhibitNewDownloadsFileName), nil, 0644)
		require.NoError(err)

		tf := NewAtomicTorrentFiles(dirs.Snap)
		prohibited, err := tf.NewDownloadsAreProhibited("v1-004900-005000-headers.seg")
		require.NoError(err)
		require.True(prohibited)
		prohibited, err = tf.NewDownloadsAreProhibited("v1-004900-005000-headers.seg.torrent")
		require.NoError(err)
		require.True(prohibited)
	})
	t.Run("prev version .lock upgrade", func(t *testing.T) {
		//old lock
		err := os.WriteFile(filepath.Join(dirs.Snap, ProhibitNewDownloadsFileName), nil, 0644)
		require.NoError(err)

		tf := NewAtomicTorrentFiles(dirs.Snap)
		err = tf.prohibitNewDownloads("transactions") //upgrade
		require.NoError(err)

		prohibited, err := tf.NewDownloadsAreProhibited("v1-004900-005000-headers.seg")
		require.NoError(err)
		require.False(prohibited)
		prohibited, err = tf.NewDownloadsAreProhibited("v1-004900-005000-headers.seg.torrent")
		require.NoError(err)
		require.False(prohibited)
	})
}
