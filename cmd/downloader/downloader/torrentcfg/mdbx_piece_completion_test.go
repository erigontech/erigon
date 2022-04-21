package torrentcfg

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

func TestBoltPieceCompletion(t *testing.T) {
	db := memdb.NewTestDownloaderDB(t)
	pc, err := NewBoltPieceCompletion(db)
	require.NoError(t, err)
	defer pc.Close()

	pk := metainfo.PieceKey{}

	b, err := pc.Get(pk)
	require.NoError(t, err)
	assert.False(t, b.Ok)

	require.NoError(t, pc.Set(pk, false))

	b, err = pc.Get(pk)
	require.NoError(t, err)
	assert.Equal(t, storage.Completion{Complete: false, Ok: true}, b)

	require.NoError(t, pc.Set(pk, true))

	b, err = pc.Get(pk)
	require.NoError(t, err)
	assert.Equal(t, storage.Completion{Complete: true, Ok: true}, b)
}
