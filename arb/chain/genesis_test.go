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

func TestArbOneGenesisToBlock(t *testing.T) {
	block, _, err := genesiswrite.GenesisToBlock(t, ArbOneGenesis(), datadir.New(t.TempDir()), log.Root())
	require.NoError(t, err)
	require.Equal(t, uint64(0x152dd49), block.NumberU64())
	require.Equal(t, Arb1GenesisStateRoot, block.Root(),
		"arb1 genesis state root mismatch: got %s, want %s", block.Root(), Arb1GenesisStateRoot)
	require.Equal(t, Arb1GenesisHash, block.Hash(),
		"arb1 genesis hash mismatch: got %s, want %s", block.Hash(), Arb1GenesisHash)
}
