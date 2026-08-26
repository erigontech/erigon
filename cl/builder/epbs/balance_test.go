package epbs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestCheckBalanceUsesConsensusSpendableBalance(t *testing.T) {
	mgr, _ := newTestManager(t)
	cfg := *mgr.beaconCfg
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ())
	builder := &cltypes.Builder{
		Pubkey: mgr.Pubkey(), Balance: cfg.MinDepositAmount,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	}
	builders.Append(builder)
	s.SetBuilders(builders)
	sd := &mockSyncedData{state: s}

	status, err := CheckBalance(sd, 0, mgr.Pubkey())
	require.NoError(t, err)
	require.True(t, status.Active)
	require.Zero(t, status.Balance)

	builder.Balance = cfg.MinDepositAmount + 100
	withdrawals := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
		int(cfg.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ(),
	)
	withdrawals.Append(&cltypes.BuilderPendingWithdrawal{BuilderIndex: 0, Amount: 60})
	s.SetBuilderPendingWithdrawals(withdrawals)
	status, err = CheckBalance(sd, 0, mgr.Pubkey())
	require.NoError(t, err)
	require.Equal(t, uint64(40), status.Balance)
}

func TestCheckBalanceRejectsCachedIndexForDifferentPubkey(t *testing.T) {
	mgr, _ := newTestManager(t)
	cfg := *mgr.beaconCfg
	s := state.New(&cfg)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{Pubkey: common.Bytes48{1}, Balance: cfg.MinDepositAmount + 100})
	s.SetBuilders(builders)

	_, err := CheckBalance(&mockSyncedData{state: s}, 0, mgr.Pubkey())
	require.ErrorIs(t, err, ErrBuilderIndexMismatch)
}
