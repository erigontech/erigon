package cltypes

import (
	"testing"

	"github.com/erigontech/erigon/common"
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
