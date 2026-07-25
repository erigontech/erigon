package devvalidator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/devgenesis"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

// TestForkVersionForEpoch_DevMode verifies that forkVersionForEpoch returns
// the correct (latest active) fork version, not GenesisForkVersion, when
// all forks are activated at epoch 0.
func TestForkVersionForEpoch_DevMode(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	// Simulate dev mode: all forks at epoch 0.
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0

	fv := forkVersionForEpoch(0, &cfg)

	// Must NOT be GenesisForkVersion.
	genesisVersion := common.Bytes4(utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)))
	require.NotEqual(t, genesisVersion, fv,
		"dev mode epoch 0 should use the latest fork version, not GenesisForkVersion")

	// Must be FuluForkVersion (the latest fork with epoch <= 0).
	expected := common.Bytes4(utils.Uint32ToBytes4(uint32(cfg.FuluForkVersion)))
	require.Equal(t, expected, fv)
}

// TestForkVersionForEpoch_MainnetProgression verifies that on mainnet config
// (where forks activate at different epochs), the version progresses correctly.
func TestForkVersionForEpoch_MainnetProgression(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	// Before Altair → GenesisForkVersion.
	fv := forkVersionForEpoch(0, &cfg)
	require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion))), fv)

	// At Altair epoch → AltairForkVersion.
	fv = forkVersionForEpoch(cfg.AltairForkEpoch, &cfg)
	require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(uint32(cfg.AltairForkVersion))), fv)

	// At Deneb epoch → DenebForkVersion.
	fv = forkVersionForEpoch(cfg.DenebForkEpoch, &cfg)
	require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(uint32(cfg.DenebForkVersion))), fv)
}

// TestSigningDomainMatchesGenesis verifies that the domain computed by
// signObject at epoch 0 matches the domain derived from the genesis state's
// Fork.CurrentVersion. This is the core invariant: if these two diverge,
// every block proposal and attestation in dev mode will be rejected.
func TestSigningDomainMatchesGenesis(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0

	genesisTime := uint64(1000)
	elHash := common.Hash{}

	state, _, err := devgenesis.BuildGenesisState("test", 4, &cfg, genesisTime, elHash)
	require.NoError(t, err)

	gvr := state.GenesisValidatorsRoot()

	// Domain from genesis state's Fork.CurrentVersion.
	stateForkVersion := state.Fork().CurrentVersion
	stateDomain, err := fork.ComputeDomain(
		cfg.DomainBeaconProposer[:], stateForkVersion, gvr,
	)
	require.NoError(t, err)

	// Domain from signing code's forkVersionForEpoch.
	signingForkVersion := forkVersionForEpoch(0, &cfg)
	signingDomain, err := fork.ComputeDomain(
		cfg.DomainBeaconProposer[:], signingForkVersion, gvr,
	)
	require.NoError(t, err)

	require.Equal(t, stateDomain, signingDomain,
		"signing domain must match genesis state domain at epoch 0")
}
