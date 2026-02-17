package chain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/db/datadir"
)

func TestArbSepoliaGenesisToBlock(t *testing.T) {
	block, _, err := genesiswrite.GenesisToBlock(t, ArbSepoliaRollupGenesisBlock(), datadir.New(t.TempDir()), log.Root())
	require.NoError(t, err)
	require.Equal(t, ArbSepoliaGenesisHash, block.Hash(),
		"arb-sepolia genesis hash mismatch: got %s, want %s", block.Hash(), ArbSepoliaGenesisHash)
}
