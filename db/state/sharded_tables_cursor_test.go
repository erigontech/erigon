package state

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/stretchr/testify/require"
)

// are seeks always done from start?

func prepareData(t *testing.T, db kv.RwDB) {
	// non dupsort
	// key||txnum -> value
	t.Helper()
	tables := []string{"sharded_table_1023", "sharded_table_1024", "sharded_table_1025"}
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	var key [8]byte
	for i, tbl := range tables {
		c, err := tx.RwCursor(tbl)
		require.NoError(t, err)
		defer c.Close()
		// insert 10 keys per table
		for j := 0; j < 10; j++ {
			binary.BigEndian.PutUint64(key[:], uint64(i*10+j))
			value := []byte{byte(i*10 + j)}
			err = c.Put(key[:], value)
			require.NoError(t, err)
		}
	}

	require.NoError(t, tx.Commit())

	0 - 9
	10 - 19
	20 - 29
}

func TestNext(t *testing.T) {

}
