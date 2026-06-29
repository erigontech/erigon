package eth2_test

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
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

func TestProcessProposerSlashingDoesNotClearDifferentProposerPayment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := &clparams.MainnetBeaconConfig
	slot := uint64(3)
	proposerIndex := uint64(7)
	paymentIndex := int(cfg.SlotsPerEpoch + slot%cfg.SlotsPerEpoch)
	payments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * cfg.SlotsPerEpoch))
	payments.Set(paymentIndex, &cltypes.BuilderPendingPayment{
		Withdrawal: &cltypes.BuilderPendingWithdrawal{
			Amount:       100,
			BuilderIndex: 1,
		},
		ProposerIndex: proposerIndex + 1,
	})
	validator := solid.NewValidatorFromParameters(
		common.Bytes48{},
		common.Hash{},
		cfg.MinActivationBalance,
		false,
		0,
		0,
		cfg.FarFutureEpoch,
		cfg.FarFutureEpoch,
	)
	s := mock_services.NewMockBeaconState(ctrl)
	s.EXPECT().ValidatorForValidatorIndex(int(proposerIndex)).Return(validator, nil)
	s.EXPECT().BeaconConfig().Return(cfg).AnyTimes()
	s.EXPECT().Slot().Return(slot).AnyTimes()
	s.EXPECT().Version().Return(clparams.GloasVersion)
	s.EXPECT().GetBuilderPendingPayments().Return(payments)
	s.EXPECT().SetBuilderPendingPayments(payments)
	s.EXPECT().SlashValidator(proposerIndex, nil).Return(uint64(0), nil)

	machine := &eth2.Impl{}
	err := machine.ProcessProposerSlashing(s, newTestProposerSlashing(slot, proposerIndex))

	require.NoError(t, err)
	require.Equal(t, uint64(100), payments.Get(paymentIndex).Withdrawal.Amount)
	require.Equal(t, proposerIndex+1, payments.Get(paymentIndex).ProposerIndex)
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
		PubKey: pubkey,
		Amount: 25,
	})

	require.NoError(t, err)
	require.Equal(t, 1, s.GetBuilders().Len())
	require.Equal(t, uint64(125), s.GetBuilders().Get(0).Balance)
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
