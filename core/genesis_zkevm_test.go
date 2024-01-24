package core_test

import (
	"github.com/ledgerwatch/erigon/params"
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
)

func TestHermezBlockRoots(t *testing.T) {
	require := require.New(t)

	t.Run("Hermez Mainnet", func(t *testing.T) {
		block, _, err := core.GenesisToBlock(core.HermezMainnetGenesisBlock(), "")
		require.NoError(err)
		if block.Root() != params.HermezMainnetGenesisHash {
			t.Errorf("wrong Hermez Mainnet genesis state root, got %v, want %v", block.Root(), params.HermezMainnetGenesisHash)
		}
	})

	t.Run("Hermez Testnet", func(t *testing.T) {
		block, _, err := core.GenesisToBlock(core.HermezTestnetGenesisBlock(), "")
		require.NoError(err)
		if block.Root() != params.HermezTestnetGenesisHash {
			t.Errorf("wrong Hermez Testnet genesis state root, got %v, want %v", block.Root(), params.HermezTestnetGenesisHash)
		}
	})

	t.Run("Hermez Blueberry", func(t *testing.T) {
		block, _, err := core.GenesisToBlock(core.HermezBlueberryGenesisBlock(), "")
		require.NoError(err)
		if block.Root() != params.HermezBlueberryGenesisHash {
			t.Errorf("wrong Hermez Testnet genesis state root, got %v, want %v", block.Root(), params.HermezTestnetGenesisHash)
		}
	})

	t.Run("Hermez Cardona", func(t *testing.T) {
		block, _, err := core.GenesisToBlock(core.HermezCardonaGenesisBlock(), "")
		require.NoError(err)
		if block.Root() != params.HermezCardonaGenesisHash {
			t.Errorf("wrong Hermez Cardona genesis state root, got %v, want %v", block.Root(), params.HermezCardonaGenesisHash)
		}
	})

	t.Run("Hermez Cardona Internal", func(t *testing.T) {
		block, _, err := core.GenesisToBlock(core.HermezCardonaInternalGenesisBlock(), "")
		require.NoError(err)
		if block.Root() != params.HermezCardonaInternalGenesisHash {
			t.Errorf("wrong Hermez Cardona Internal genesis state root, got %v, want %v", block.Root(), params.HermezCardonaInternalGenesisHash)
		}
	})
}

func TestX1BlockRoots(t *testing.T) {
	require := require.New(t)
	t.Run("X1 Testnet", func(t *testing.T) {
		block, _, err := core.GenesisToBlock(core.X1TestnetGenesisBlock(), "")
		require.NoError(err)
		if block.Root() != params.X1TestnetGenesisHash {
			t.Errorf("wrong X1 Testnet genesis state root, got %v, want %v", block.Root(), params.X1TestnetGenesisHash)
		}
	})
}

func TestCommitGenesisIdempotency2(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	genesis := core.GenesisBlockByChainName(networkname.HermezMainnetChainName)
	_, _, err := core.WriteGenesisBlock(tx, genesis, nil, "")
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, "")
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}
