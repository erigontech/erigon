package services

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func TestExecutionPayloadBidServiceFirstGloasBidDoesNotRequireFuluEnvelope(t *testing.T) {
	service, _, clock, fc, _ := setupExecutionPayloadBidService(t, gomock.NewController(t))
	cfg := service.beaconCfg
	cfg.GloasForkEpoch = 3
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	msg := newTestSignedExecutionPayloadBid(96, 0, 1000)
	clock.EXPECT().GetCurrentSlot().Return(uint64(96)).AnyTimes()
	fc.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fc.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{Slot: 95}
	parent := state2.New(cfg)
	parent.SetVersion(clparams.FuluVersion)
	require.NoError(t, parent.SetSlot(95))
	parent.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 95})
	parent.SetLatestExecutionPayloadHeader(&cltypes.Eth1Header{BlockHash: msg.Message.ParentBlockHash})
	parent.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 2})
	for i := range 64 {
		pk := common.Bytes48{byte(i)}
		validator := solid.NewValidatorFromParameters(pk, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch)
		require.NoError(t, parent.AddValidator(validator, cfg.MaxEffectiveBalance))
	}
	parent.SetInactivityScores(make([]uint64, 64))
	parent.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, 64))
	parent.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, 64))
	parent.SetProposerLookahead(solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch)))
	key, err := bls.GenerateKey()
	require.NoError(t, err)
	deposit := &solid.PendingDeposit{Slot: 0, Amount: state2.GetActivationExitChurnLimit(parent) + cfg.MinDepositAmount}
	copy(deposit.PubKey[:], bls.CompressPublicKey(key.PublicKey()))
	deposit.WithdrawalCredentials[0] = byte(cfg.BuilderWithdrawalPrefix)
	data := &cltypes.DepositData{PubKey: deposit.PubKey, WithdrawalCredentials: deposit.WithdrawalCredentials, Amount: deposit.Amount}
	messageRoot, err := data.MessageHash()
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(cfg.DomainDeposit[:], utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)), [32]byte{})
	require.NoError(t, err)
	signingRoot := crypto.Sha256(messageRoot[:], domain)
	copy(deposit.Signature[:], key.Sign(signingRoot[:]).Bytes())
	parent.GetPendingDeposits().Append(deposit)
	fc.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = parent
	bidErr := service.ValidateBid(t.Context(), msg)
	require.NoError(t, service.ValidateBid(t.Context(), msg))
	entry, err := service.bidValidationState(msg.Message.ParentBlockRoot, msg.Message.Slot)
	require.NoError(t, err)
	require.Equal(t, clparams.FuluVersion, parent.Version())
	require.Equal(t, clparams.GloasVersion, entry.state.Version())
	require.Equal(t, 1, entry.state.GetBuilders().Len())
	require.True(t, state2.IsActiveBuilder(entry.state, 0))
	require.Equal(t, msg.Message.ParentBlockHash, entry.state.GetLatestExecutionPayloadBid().BlockHash)
	require.Empty(t, fc.Envelopes)
	require.NoError(t, bidErr)
}
