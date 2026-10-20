package eth2_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

func TestProcessExecutionPayloadEnvelopeRejectsNilEnvelope(t *testing.T) {
	s := state.New(&clparams.MainnetBeaconConfig)
	machine := &eth2.Impl{}

	require.Error(t, machine.ProcessExecutionPayloadEnvelope(s, nil))
	require.Error(t, machine.ProcessExecutionPayloadEnvelope(s, &cltypes.SignedExecutionPayloadEnvelope{}))
	require.Error(t, machine.ProcessExecutionPayloadEnvelope(s, &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{},
	}))
}

func TestApplyParentExecutionPayloadUsesHeaderSlot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	require.NoError(t, s.SetSlot(65))
	s.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 63})
	s.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{Slot: 0, BlockHash: common.Hash{1}})
	payments := s.GetBuilderPendingPayments()
	payments.Set(31, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{Amount: 17}})
	s.SetBuilderPendingPayments(payments)

	machine := &eth2.Impl{}
	require.NoError(t, machine.ApplyParentExecutionPayload(s, &cltypes.ExecutionRequests{}))
	require.Equal(t, 1, s.GetBuilderPendingWithdrawals().Len())
	require.Equal(t, uint64(17), s.GetBuilderPendingWithdrawals().Get(0).Amount)
	require.Zero(t, s.GetBuilderPendingPayments().Get(31).Withdrawal.Amount)
	require.True(t, s.GetExecutionPayloadAvailability().GetBitAt(63))
	require.False(t, s.GetExecutionPayloadAvailability().GetBitAt(0))
	require.Equal(t, common.Hash{1}, s.GetLatestBlockHash())
}

func TestProcessBuilderDepositRequestTopsUpExistingBuilder(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	pubkey := common.Bytes48{0x11}
	builders.Append(&cltypes.Builder{
		Pubkey:            pubkey,
		Version:           cfg.PayloadBuilderVersion,
		ExecutionAddress:  common.HexToAddress("0x0000000000000000000000000000000000001234"),
		Balance:           100,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	s.SetBuilders(builders)

	machine := &eth2.Impl{}
	err := machine.ProcessBuilderDepositRequest(s, &solid.BuilderDepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: common.Hash{byte(cfg.BuilderWithdrawalPrefix)},
		Amount:                25,
	})

	require.NoError(t, err)
	require.Equal(t, 1, s.GetBuilders().Len())
	require.Equal(t, uint64(125), s.GetBuilders().Get(0).Balance)
}

func TestProcessBuilderDepositRequestRejectsBalanceOverflow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	pubkey := common.Bytes48{0x11}
	builders.Append(&cltypes.Builder{
		Pubkey:            pubkey,
		Version:           cfg.PayloadBuilderVersion,
		Balance:           math.MaxUint64,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	s.SetBuilders(builders)

	machine := &eth2.Impl{}
	err := machine.ProcessBuilderDepositRequest(s, &solid.BuilderDepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: common.Hash{byte(cfg.BuilderWithdrawalPrefix)},
		Amount:                1,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "builder balance overflow")
	require.Equal(t, uint64(math.MaxUint64), s.GetBuilders().Get(0).Balance)
}

func TestProcessBuilderExitRequestInitiatesActiveBuilderExit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	pubkey := common.Bytes48{0x11}
	sourceAddress := common.HexToAddress("0x0000000000000000000000000000000000001234")
	builders.Append(&cltypes.Builder{
		Pubkey:            pubkey,
		Version:           cfg.PayloadBuilderVersion,
		ExecutionAddress:  sourceAddress,
		Balance:           100,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	s.SetBuilders(builders)

	machine := &eth2.Impl{}
	err := machine.ProcessBuilderExitRequest(s, &solid.BuilderExitRequest{
		SourceAddress: sourceAddress,
		PubKey:        pubkey,
	})

	require.NoError(t, err)
	require.NotEqual(t, cfg.FarFutureEpoch, s.GetBuilders().Get(0).WithdrawableEpoch)
}

func TestWithdrawalRequestDoesNotInitiateBuilderExit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	pubkey := common.Bytes48{0x11}
	sourceAddress := common.HexToAddress("0x0000000000000000000000000000000000001234")
	builders.Append(&cltypes.Builder{
		Pubkey:            pubkey,
		Version:           cfg.PayloadBuilderVersion,
		ExecutionAddress:  sourceAddress,
		Balance:           100,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	s.SetBuilders(builders)

	machine := &eth2.Impl{}
	err := machine.ProcessWithdrawalRequest(s, &solid.WithdrawalRequest{
		SourceAddress:   sourceAddress,
		ValidatorPubKey: pubkey,
		Amount:          0,
	})

	require.NoError(t, err)
	require.Equal(t, cfg.FarFutureEpoch, s.GetBuilders().Get(0).WithdrawableEpoch)
}

func TestProcessBuilderRequestsRejectNil(t *testing.T) {
	machine := &eth2.Impl{}
	s := state.New(&clparams.MainnetBeaconConfig)

	require.Error(t, machine.ProcessBuilderDepositRequest(s, nil))
	require.Error(t, machine.ProcessBuilderExitRequest(s, nil))
}

func TestProcessPayloadAttestationRejectsNil(t *testing.T) {
	machine := &eth2.Impl{}
	s := state.New(&clparams.MainnetBeaconConfig)

	require.Error(t, machine.ProcessPayloadAttestation(s, nil))
	require.Error(t, machine.ProcessPayloadAttestation(s, &cltypes.PayloadAttestation{}))
	require.Error(t, machine.ProcessPayloadAttestation(s, &cltypes.PayloadAttestation{
		Data: &cltypes.PayloadAttestationData{},
	}))
}

func TestProcessVoluntaryExitRejectsBuilderIndex(t *testing.T) {
	machine := &eth2.Impl{}
	s := state.New(&clparams.MainnetBeaconConfig)
	s.SetVersion(clparams.GloasVersion)

	err := machine.ProcessVoluntaryExit(s, &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{
			ValidatorIndex: state.ConvertBuilderIndexToValidatorIndex(0),
		},
	})

	require.Error(t, err)
}

func newTestProposerSlashing(slot, proposerIndex uint64) *cltypes.ProposerSlashing {
	return &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          slot,
				ProposerIndex: proposerIndex,
				BodyRoot:      common.HexToHash("0x01"),
			},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          slot,
				ProposerIndex: proposerIndex,
				BodyRoot:      common.HexToHash("0x02"),
			},
		},
	}
}
