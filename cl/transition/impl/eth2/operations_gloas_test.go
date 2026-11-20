package eth2_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
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

func TestProcessExecutionPayloadEnvelopeRejectsNilTransactions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		machine *eth2.Impl
	}{
		{name: "default", machine: transition.DefaultMachine},
		{name: "validating", machine: transition.ValidatingMachine},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, envelope := validProcessExecutionPayloadEnvelopeFixture(t)
			envelope.Message.Payload.Transactions = nil

			err := tc.machine.ProcessExecutionPayloadEnvelope(s, envelope)

			require.EqualError(t, err, "ProcessExecutionPayloadEnvelope: payload has nil transactions")
		})
	}
}

func TestProcessExecutionPayloadEnvelopeDoesNotBuildPayloadHeader(t *testing.T) {
	s, envelope := validProcessExecutionPayloadEnvelopeFixture(t)
	envelope.Message.Payload.Withdrawals = nil

	err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(s, envelope)

	require.EqualError(t, err, "ProcessExecutionPayloadEnvelope: payload has nil withdrawals")
}

func TestProcessExecutionPayloadEnvelopeScalarChecks(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*state.CachingBeaconState, *cltypes.SignedExecutionPayloadEnvelope)
		wantError string
	}{
		{name: "valid"},
		{
			name: "prev randao",
			mutate: func(_ *state.CachingBeaconState, envelope *cltypes.SignedExecutionPayloadEnvelope) {
				envelope.Message.Payload.PrevRandao[0]++
			},
			wantError: "ProcessExecutionPayloadEnvelope: prev_randao mismatch with committed bid",
		},
		{
			name: "gas limit",
			mutate: func(_ *state.CachingBeaconState, envelope *cltypes.SignedExecutionPayloadEnvelope) {
				envelope.Message.Payload.GasLimit++
			},
			wantError: "ProcessExecutionPayloadEnvelope: gas_limit 30000001 != committed bid gas_limit 30000000",
		},
		{
			name: "block hash",
			mutate: func(_ *state.CachingBeaconState, envelope *cltypes.SignedExecutionPayloadEnvelope) {
				envelope.Message.Payload.BlockHash[0]++
			},
			wantError: "ProcessExecutionPayloadEnvelope: block_hash mismatch with committed bid",
		},
		{
			name: "parent hash",
			mutate: func(_ *state.CachingBeaconState, envelope *cltypes.SignedExecutionPayloadEnvelope) {
				envelope.Message.Payload.ParentHash[0]++
			},
			wantError: "ProcessExecutionPayloadEnvelope: parent_hash mismatch with latest block hash",
		},
		{
			name: "timestamp",
			mutate: func(_ *state.CachingBeaconState, envelope *cltypes.SignedExecutionPayloadEnvelope) {
				envelope.Message.Payload.Time++
			},
			wantError: "ProcessExecutionPayloadEnvelope: invalid timestamp",
		},
		{
			name: "slot number",
			mutate: func(_ *state.CachingBeaconState, envelope *cltypes.SignedExecutionPayloadEnvelope) {
				envelope.Message.Payload.SlotNumber++
			},
			wantError: "ProcessExecutionPayloadEnvelope: payload.slot_number 65 != state slot 64",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, envelope := validProcessExecutionPayloadEnvelopeFixture(t)
			if tc.mutate != nil {
				tc.mutate(s, envelope)
			}
			beforeRoot, err := s.HashSSZ()
			require.NoError(t, err)
			beforePreviousRoot := s.PeekPreviousStateRoot()

			err = transition.DefaultMachine.ProcessExecutionPayloadEnvelope(s, envelope)

			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantError)
			}
			afterRoot, hashErr := s.HashSSZ()
			require.NoError(t, hashErr)
			require.Equal(t, beforeRoot, afterRoot)
			require.Equal(t, beforePreviousRoot, s.PeekPreviousStateRoot())
		})
	}
}

func TestProcessExecutionPayloadEnvelopeValidatingMachineRejectsInvalidSignature(t *testing.T) {
	s, envelope := validProcessExecutionPayloadEnvelopeFixture(t)
	wrongKey, err := bls.NewPrivateKeyFromIKM([]byte("abcdefghijklmnopqrstuvwxyz123456"))
	require.NoError(t, err)
	signProcessExecutionPayloadEnvelope(t, s, envelope, wrongKey)

	err = transition.ValidatingMachine.ProcessExecutionPayloadEnvelope(s, envelope)

	require.EqualError(t, err, "ProcessExecutionPayloadEnvelope: invalid envelope signature")
}

func validProcessExecutionPayloadEnvelopeFixture(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedExecutionPayloadEnvelope) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.GloasForkEpoch = 0
	cfg.GloasForkVersion = 0x80000038
	cfg.InitializeForkSchedule()

	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	require.NoError(t, s.SetSlot(64))
	s.SetGenesisValidatorsRoot(common.HexToHash("0x01"))
	s.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
	})
	privateKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	publicKey := common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey()))
	require.NoError(t, s.AddValidator(solid.NewValidatorFromParameters(publicKey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance))
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	builders.Append(&cltypes.Builder{Pubkey: publicKey})
	s.SetBuilders(builders)

	parentBeaconRoot := common.HexToHash("0x11")
	stateRoot := common.HexToHash("0x22")
	s.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          s.Slot(),
		ProposerIndex: 0,
		ParentRoot:    parentBeaconRoot,
		BodyRoot:      common.HexToHash("0x33"),
	})
	s.SetPreviousStateRoot(stateRoot)
	header := s.LatestBlockHeader()
	header.Root = stateRoot
	beaconBlockRoot, err := header.HashSSZ()
	require.NoError(t, err)

	requests := cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	withdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.ParentHash = common.HexToHash("0x44")
	payload.PrevRandao = common.HexToHash("0x55")
	payload.BlockHash = common.HexToHash("0x66")
	payload.GasLimit = 30_000_000
	payload.SlotNumber = s.Slot()
	payload.Time = state.ComputeTimestampAtSlot(s, s.Slot())
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = withdrawals
	payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	s.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		BlockHash:             payload.BlockHash,
		BuilderIndex:          0,
		Slot:                  payload.SlotNumber,
		PrevRandao:            payload.PrevRandao,
		GasLimit:              payload.GasLimit,
		ExecutionRequestsRoot: requestsRoot,
	})
	s.SetLatestBlockHash(payload.ParentHash)
	s.SetPayloadExpectedWithdrawals(withdrawals)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload:               payload,
		ExecutionRequests:     requests,
		BuilderIndex:          0,
		BeaconBlockRoot:       beaconBlockRoot,
		ParentBeaconBlockRoot: parentBeaconRoot,
	}}
	signProcessExecutionPayloadEnvelope(t, s, envelope, privateKey)
	return s, envelope
}

func signProcessExecutionPayloadEnvelope(
	t *testing.T,
	s *state.CachingBeaconState,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	privateKey *bls.PrivateKey,
) {
	t.Helper()
	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconBuilder, state.Epoch(s))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(envelope.Message, domain)
	require.NoError(t, err)
	copy(envelope.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
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
