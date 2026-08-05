package cltypes

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
	"github.com/stretchr/testify/require"
)

func TestSignedExecutionPayloadEnvelopeCloneNilMessage(t *testing.T) {
	envelope := &SignedExecutionPayloadEnvelope{
		Signature: common.Bytes96{1, 2, 3},
	}

	cloned := envelope.Clone().(*SignedExecutionPayloadEnvelope)
	require.Nil(t, cloned.Message)
	require.Equal(t, envelope.Signature, cloned.Signature)
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
