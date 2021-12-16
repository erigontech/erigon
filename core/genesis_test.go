package core

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/params"
)

func TestDefaultBSCGenesisBlock(t *testing.T) {
	genesis := DefaultBSCGenesisBlock()
	db := memdb.New()
	_, block, err := CommitGenesisBlock(db, genesis)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), params.BSCGenesisHash)
}
