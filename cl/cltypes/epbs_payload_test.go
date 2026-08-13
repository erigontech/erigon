package cltypes

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

func TestExecutionRequestsStrictDecodeRejectsNonCanonicalOffset(t *testing.T) {
	requests := NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	encoded, err := requests.EncodeSSZ(nil)
	require.NoError(t, err)
	firstOffset := binary.LittleEndian.Uint32(encoded)
	binary.LittleEndian.PutUint32(encoded, firstOffset+1)
	encoded = append(encoded[:firstOffset], append([]byte{0}, encoded[firstOffset:]...)...)

	decoded := NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	require.Error(t, decoded.DecodeSSZStrict(encoded, int(clparams.GloasVersion)))
}

func TestParseExecutionPayloadEnvelopeVersion(t *testing.T) {
	for _, header := range []string{"", "GLOAS", "glamsterdam"} {
		version, err := ParseExecutionPayloadEnvelopeVersion(header)
		require.NoError(t, err)
		require.Equal(t, clparams.GloasVersion, version)
	}
	for _, header := range []string{"fulu", "unknown"} {
		_, err := ParseExecutionPayloadEnvelopeVersion(header)
		require.Error(t, err)
	}
}

func TestValidateExecutionPayloadEnvelopeVersion(t *testing.T) {
	require.NoError(t, ValidateExecutionPayloadEnvelopeVersion(clparams.GloasVersion))
	require.Error(t, ValidateExecutionPayloadEnvelopeVersion(clparams.FuluVersion))
	require.Error(t, ValidateExecutionPayloadEnvelopeVersion(clparams.StateVersion(255)))
}

func TestSignedExecutionPayloadEnvelopeCloneNilMessage(t *testing.T) {
	envelope := &SignedExecutionPayloadEnvelope{
		Signature: common.Bytes96{1, 2, 3},
	}

	cloned := envelope.Clone().(*SignedExecutionPayloadEnvelope)
	require.Nil(t, cloned.Message)
	require.Equal(t, envelope.Signature, cloned.Signature)
}

func TestExecutionPayloadEnvelopeValidationSeparatesProtocolAndPersistenceBounds(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalsPerPayload = 1
	cfg.MaxWithdrawalRequestsPerPayload = 1
	cfg.MaxConsolidationRequestsPerPayload = 1
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	cfg.MaxBuilderExitRequestsPerPayload = 1
	cfg.MaxTransactionsPerPayload = 1
	cfg.MaxBytesPerTransaction = 1

	for _, test := range []struct {
		name   string
		mutate func(*SignedExecutionPayloadEnvelope)
	}{
		{"payload withdrawals", func(e *SignedExecutionPayloadEnvelope) {
			e.Message.Payload.Withdrawals.Append(&Withdrawal{})
			e.Message.Payload.Withdrawals.Append(&Withdrawal{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope := validTestExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
			test.mutate(envelope)
			require.Error(t, envelope.ValidateForConfig(&cfg))
		})
	}

	envelope := validTestExecutionPayloadEnvelope(&cfg)
	for range 16_385 {
		envelope.Message.ExecutionRequests.Deposits.Append(&solid.DepositRequest{})
	}
	require.NoError(t, envelope.ValidateForConfig(&cfg))
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Less(t, uint64(len(encoded)), clparams.MaxChunkSize)
	require.NoError(t, envelope.ValidateForPersistence(&cfg))

	decoded := &SignedExecutionPayloadEnvelope{Message: NewExecutionPayloadEnvelope(&cfg)}
	require.NoError(t, decoded.DecodeSSZStrict(encoded, int(clparams.GloasVersion)))
	require.Equal(t, 16_385, decoded.Message.ExecutionRequests.Deposits.Len())

	for _, test := range []struct {
		name   string
		mutate func(*SignedExecutionPayloadEnvelope)
	}{
		{"transactions", func(e *SignedExecutionPayloadEnvelope) {
			e.Message.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}})
		}},
		{"block access list", func(e *SignedExecutionPayloadEnvelope) {
			require.NoError(t, e.Message.Payload.BlockAccessList.SetBytes([]byte{1, 2}))
		}},
	} {
		t.Run(test.name+" are resource bounded only", func(t *testing.T) {
			envelope := validTestExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
			test.mutate(envelope)
			require.NoError(t, envelope.ValidateForConfig(&cfg))
			require.Error(t, envelope.ValidateForPersistence(&cfg))
		})
	}
}

func TestExecutionPayloadEnvelopeValidateForPersistenceRejectsOversizedEncoding(t *testing.T) {
	envelope := validTestExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	for range progressiveRequestDecodeLimit(solid.SizeDepositRequest) {
		envelope.Message.ExecutionRequests.Deposits.Append(&solid.DepositRequest{})
	}

	require.Greater(t, uint64(envelope.EncodingSizeSSZ()), clparams.MaxChunkSize)
	require.ErrorContains(t, envelope.ValidateForPersistence(&clparams.MainnetBeaconConfig), "exceeds max")
}

func TestExecutionPayloadEnvelopeValidateForPersistenceAllowsProgressiveListsPastConfiguredLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalRequestsPerPayload = 1
	cfg.MaxConsolidationRequestsPerPayload = 1
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	cfg.MaxBuilderExitRequestsPerPayload = 1

	for _, test := range []struct {
		name   string
		mutate func(*SignedExecutionPayloadEnvelope)
	}{
		{"withdrawals", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
			envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
		}},
		{"consolidations", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.Consolidations.Append(&solid.ConsolidationRequest{})
			envelope.Message.ExecutionRequests.Consolidations.Append(&solid.ConsolidationRequest{})
		}},
		{"builder deposits", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
			envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
		}},
		{"builder exits", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.BuilderExits.Append(&solid.BuilderExitRequest{})
			envelope.Message.ExecutionRequests.BuilderExits.Append(&solid.BuilderExitRequest{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope := validTestExecutionPayloadEnvelope(&cfg)
			test.mutate(envelope)

			require.NoError(t, envelope.ValidateForPersistence(&cfg))
		})
	}
}

func TestExecutionPayloadEnvelopeValidateForConfigRejectsGloasRequestsPastConsensusLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalRequestsPerPayload = 1
	cfg.MaxConsolidationRequestsPerPayload = 1
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	cfg.MaxBuilderExitRequestsPerPayload = 1

	for _, test := range []struct {
		name   string
		mutate func(*SignedExecutionPayloadEnvelope)
	}{
		{"withdrawals", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
			envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
		}},
		{"consolidations", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.Consolidations.Append(&solid.ConsolidationRequest{})
			envelope.Message.ExecutionRequests.Consolidations.Append(&solid.ConsolidationRequest{})
		}},
		{"builder deposits", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
			envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
		}},
		{"builder exits", func(envelope *SignedExecutionPayloadEnvelope) {
			envelope.Message.ExecutionRequests.BuilderExits.Append(&solid.BuilderExitRequest{})
			envelope.Message.ExecutionRequests.BuilderExits.Append(&solid.BuilderExitRequest{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope := validTestExecutionPayloadEnvelope(&cfg)
			test.mutate(envelope)

			require.Error(t, envelope.ValidateForConfig(&cfg))
		})
	}
}

func TestExecutionRequestsElectraDecodeKeepsConfiguredLimit(t *testing.T) {
	producerCfg := clparams.MainnetBeaconConfig
	producerCfg.MaxWithdrawalRequestsPerPayload = 2
	requests := NewExecutionRequestsWithVersion(&producerCfg, clparams.ElectraVersion)
	requests.Withdrawals.Append(&solid.WithdrawalRequest{})
	requests.Withdrawals.Append(&solid.WithdrawalRequest{})
	encoded, err := requests.EncodeSSZ(nil)
	require.NoError(t, err)

	consumerCfg := clparams.MainnetBeaconConfig
	consumerCfg.MaxWithdrawalRequestsPerPayload = 1
	decoded := NewExecutionRequestsWithVersion(&consumerCfg, clparams.ElectraVersion)
	require.Error(t, decoded.DecodeSSZStrict(encoded, int(clparams.ElectraVersion)))
}

func validTestExecutionPayloadEnvelope(cfg *clparams.BeaconChainConfig) *SignedExecutionPayloadEnvelope {
	message := NewExecutionPayloadEnvelope(cfg)
	message.Payload.Extra = solid.NewExtraData()
	message.Payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	message.Payload.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	message.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	return &SignedExecutionPayloadEnvelope{Message: message}
}

func TestBuilderPendingPaymentSSZIncludesProposerIndex(t *testing.T) {
	payment := &BuilderPendingPayment{
		Weight: 123,
		Withdrawal: &BuilderPendingWithdrawal{
			FeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Amount:       456,
			BuilderIndex: 789,
		},
		ProposerIndex: 42,
	}

	encoded, err := payment.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Len(t, encoded, payment.EncodingSizeSSZ())

	var decoded BuilderPendingPayment
	require.NoError(t, decoded.DecodeSSZ(encoded, 0))
	require.Equal(t, payment.Weight, decoded.Weight)
	require.Equal(t, payment.Withdrawal, decoded.Withdrawal)
	require.Equal(t, payment.ProposerIndex, decoded.ProposerIndex)
}

func TestBuilderPendingPaymentCloneCopiesFields(t *testing.T) {
	payment := &BuilderPendingPayment{
		Weight: 123,
		Withdrawal: &BuilderPendingWithdrawal{
			FeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Amount:       456,
			BuilderIndex: 789,
		},
		ProposerIndex: 42,
	}

	cloned := payment.Clone().(*BuilderPendingPayment)

	require.Equal(t, payment, cloned)
	require.NotSame(t, payment.Withdrawal, cloned.Withdrawal)
}

func TestExecutionPayloadBidDecodePreservesProgressiveLimit(t *testing.T) {
	encoded := encodedExecutionPayloadBidWithCommitments(t, 17)
	target := &ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](1, 48),
	}

	require.ErrorIs(t, target.DecodeSSZ(encoded, 0), ssz.ErrTooBigList)

	encoded = encodedExecutionPayloadBidWithCommitments(t, 2)
	require.NoError(t, target.DecodeSSZ(encoded, 0))
}

func TestSignedExecutionPayloadBidDecodePreservesMessageLimit(t *testing.T) {
	message := &ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](1, 48),
	}
	target := &SignedExecutionPayloadBid{Message: message}
	source := &SignedExecutionPayloadBid{Message: executionPayloadBidWithCommitments(17)}
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)

	err = target.DecodeSSZ(encoded, 0)
	require.True(t, errors.Is(err, ssz.ErrTooBigList), err)
	require.Same(t, message, target.Message)
}

func TestExecutionPayloadBidClonePreservesProgressiveLimit(t *testing.T) {
	bid := &ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](1, 48),
	}
	cloned := bid.Clone().(*ExecutionPayloadBid)
	encoded := make([]byte, 17*48)

	require.ErrorIs(t, cloned.BlobKzgCommitments.DecodeSSZ(encoded, 0), ssz.ErrTooBigList)
}

func TestSignedExecutionPayloadBidJSONInitializesStaticProgressiveList(t *testing.T) {
	source := &SignedExecutionPayloadBid{Message: executionPayloadBidWithCommitments(2)}
	input, err := json.Marshal(source)
	require.NoError(t, err)

	var decoded SignedExecutionPayloadBid
	require.NoError(t, json.Unmarshal(input, &decoded))
	got, err := decoded.EncodeSSZ(nil)
	require.NoError(t, err)
	want, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, want, got)

	var roundTrip SignedExecutionPayloadBid
	require.NoError(t, roundTrip.DecodeSSZ(got, 0))
	require.Equal(t, decoded.Message.BlobKzgCommitments.Len(), roundTrip.Message.BlobKzgCommitments.Len())
}

func TestExecutionPayloadBidJSONPreservesPreseededProgressiveLimit(t *testing.T) {
	input, err := json.Marshal(executionPayloadBidWithCommitments(2))
	require.NoError(t, err)
	target := &ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*KZGCommitment](1, 48),
	}
	require.NoError(t, json.Unmarshal(input, target))

	cloned := target.Clone().(*ExecutionPayloadBid)
	require.ErrorIs(t, cloned.BlobKzgCommitments.DecodeSSZ(make([]byte, 17*48), 0), ssz.ErrTooBigList)
}

func encodedExecutionPayloadBidWithCommitments(t *testing.T, count int) []byte {
	t.Helper()
	encoded, err := executionPayloadBidWithCommitments(count).EncodeSSZ(nil)
	require.NoError(t, err)
	return encoded
}

func executionPayloadBidWithCommitments(count int) *ExecutionPayloadBid {
	commitments := solid.NewStaticProgressiveListSSZ[*KZGCommitment](1, 48)
	for range count {
		commitments.Append(new(KZGCommitment))
	}
	return &ExecutionPayloadBid{BlobKzgCommitments: *commitments}
}
