package membatch

import (
	"context"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapmutation_Flush_Close(t *testing.T) {
	db := memdb.NewTestDB(t)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	batch := NewHashBatch(tx, nil, os.TempDir(), log.New())
	defer func() {
		batch.Close()
	}()
	assert.Equal(t, batch.size, 0)
	err = batch.Put(kv.ChaindataTables[0], []byte{1}, []byte{1})
	require.NoError(t, err)
	assert.Equal(t, batch.size, 2)
	err = batch.Put(kv.ChaindataTables[0], []byte{2}, []byte{2})
	require.NoError(t, err)
	assert.Equal(t, batch.size, 4)
	err = batch.Put(kv.ChaindataTables[0], []byte{1}, []byte{3, 2, 1, 0})
	require.NoError(t, err)
	assert.Equal(t, batch.size, 7)
	err = batch.Flush(context.Background(), tx)
	require.NoError(t, err)
	batch.Close()
	batch.Close()
}
