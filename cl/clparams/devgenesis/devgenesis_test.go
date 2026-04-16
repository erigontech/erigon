package devgenesis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
)

func TestBuildGenesisState_Minimal(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig // works with any config

	genesisTime := uint64(time.Now().Unix())
	elHash := common.HexToHash("0x1234")

	state, keys, err := BuildGenesisState("test-seed", 64, &cfg, genesisTime, elHash)
	require.NoError(t, err)
	require.Len(t, keys, 64)

	// Check basic state properties.
	require.Equal(t, uint64(0), state.Slot())
	require.Equal(t, genesisTime, state.GenesisTime())
	require.Equal(t, 64, state.ValidatorLength())

	// All validators should be active at genesis.
	for i := 0; i < state.ValidatorLength(); i++ {
		v := state.Validators().Get(i)
		require.Equal(t, uint64(0), v.ActivationEpoch(), "validator %d should be active at epoch 0", i)
		require.Equal(t, cfg.MaxEffectiveBalance, v.EffectiveBalance())
	}

	// Genesis validators root should be non-zero.
	require.NotEqual(t, common.Hash{}, state.GenesisValidatorsRoot())

	// Eth1Data should reference our EL hash.
	require.Equal(t, elHash, state.Eth1Data().BlockHash)

	// State root should be computable.
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, common.Hash(root))

	t.Logf("genesis state root: %x", root)
	t.Logf("validators root: %x", state.GenesisValidatorsRoot())
}

func TestBuildGenesisState_Deterministic(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	s1, _, err := BuildGenesisState("seed-a", 16, &cfg, 1000, common.Hash{})
	require.NoError(t, err)

	s2, _, err := BuildGenesisState("seed-a", 16, &cfg, 1000, common.Hash{})
	require.NoError(t, err)

	r1, _ := s1.HashSSZ()
	r2, _ := s2.HashSSZ()
	require.Equal(t, r1, r2, "same seed should produce same state root")

	// Different seed → different root.
	s3, _, err := BuildGenesisState("seed-b", 16, &cfg, 1000, common.Hash{})
	require.NoError(t, err)
	r3, _ := s3.HashSSZ()
	require.NotEqual(t, r1, r3, "different seed should produce different state root")
}

func TestDeriveSignerKey(t *testing.T) {
	key1, addr1, err := DeriveSignerKey("test")
	require.NoError(t, err)
	key2, addr2, err := DeriveSignerKey("test")
	require.NoError(t, err)

	// Deterministic.
	require.Equal(t, key1.D.Bytes(), key2.D.Bytes())
	require.Equal(t, addr1, addr2)

	// Non-zero address.
	require.NotEqual(t, common.Address{}, addr1)

	// Different seed → different key.
	_, addr3, err := DeriveSignerKey("other")
	require.NoError(t, err)
	require.NotEqual(t, addr1, addr3)

	t.Logf("signer address: %s", addr1.Hex())
}

func TestDeriveKeys(t *testing.T) {
	keys, err := DeriveKeys("test", 10)
	require.NoError(t, err)
	require.Len(t, keys, 10)

	// Keys should be deterministic.
	keys2, err := DeriveKeys("test", 10)
	require.NoError(t, err)
	for i := range keys {
		require.Equal(t, keys[i].Bytes(), keys2[i].Bytes())
	}

	// Different seed → different keys.
	keys3, err := DeriveKeys("other", 10)
	require.NoError(t, err)
	require.NotEqual(t, keys[0].Bytes(), keys3[0].Bytes())
}
