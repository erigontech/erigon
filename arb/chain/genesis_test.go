package chain

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArbSepoliaRollupGenesisBlock_Fields(t *testing.T) {
	genesis := ArbSepoliaRollupGenesisBlock()
	require.NotNil(t, genesis)

	require.Equal(t, ArbSepoliaChainConfig, genesis.Config, "Config must match ArbSepoliaChainConfig")
	require.Equal(t, uint64(1), genesis.Nonce)
	require.Equal(t, big.NewInt(1), genesis.Difficulty)
	require.Equal(t, big.NewInt(0x5f5e100), genesis.BaseFee)
	require.Equal(t, uint64(0x4000000000000), genesis.GasLimit)
	require.Equal(t, uint64(0), genesis.Number)
	require.Equal(t, uint64(0), genesis.Timestamp)
	require.Equal(t, uint64(0), genesis.GasUsed)
}

func TestArbSepoliaRollupGenesisBlock_AllocNonEmpty(t *testing.T) {
	genesis := ArbSepoliaRollupGenesisBlock()
	require.NotNil(t, genesis)
	require.Greater(t, len(genesis.Alloc), 10,
		"Alloc must contain many precompile/ArbOS accounts, got %d", len(genesis.Alloc))
}
