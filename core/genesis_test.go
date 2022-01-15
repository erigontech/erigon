package core

import (
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDefaultBSCGenesisBlock(t *testing.T) {
	genesis := DefaultBSCGenesisBlock()
	db := memdb.New()
	_, block, err := CommitGenesisBlock(db, genesis)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), params.BSCGenesisHash)
}
