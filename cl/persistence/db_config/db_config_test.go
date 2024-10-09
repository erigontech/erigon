package db_config

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestDBConfig(t *testing.T) {
	db := memdb.NewTestDB(t)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	defer tx.Rollback()
	require.NoError(t, err)

	c := DatabaseConfiguration{PruneDepth: 69}
	require.NoError(t, WriteConfigurationIfNotExist(context.Background(), tx, c))
	cfg, err := ReadConfiguration(context.Background(), tx)
	require.NoError(t, err)
	require.Equal(t, cfg, c)
}
