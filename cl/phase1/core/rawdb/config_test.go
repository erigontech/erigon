package rawdb_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	"github.com/stretchr/testify/require"
)

func TestBeaconDataConfig(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	cfg := new(rawdb.BeaconDataConfig)
	require.NoError(t, rawdb.WriteBeaconDataConfig(tx, cfg))

	newCfg, err := rawdb.ReadBeaconDataConfig(tx)
	require.NoError(t, err)
	require.Equal(t, cfg, newCfg)
}
