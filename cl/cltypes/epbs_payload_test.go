package cltypes

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
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
	fixedSize := uint32(len(encoded))
	for offset := 0; offset < len(encoded); offset += 4 {
		binary.LittleEndian.PutUint32(encoded[offset:], fixedSize+1)
	}
	encoded = append(encoded, 0)

	decoded := NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	decoded = NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
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

func TestValidateExecutionPayloadEnvelopeBuilderIndex(t *testing.T) {
	block := NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	envelope := &SignedExecutionPayloadEnvelope{Message: NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BuilderIndex = 7
	envelope.Message.BuilderIndex = 8

	require.ErrorContains(t, ValidateExecutionPayloadEnvelopeBuilderIndex(block, envelope), "does not match")
	envelope.Message.BuilderIndex = 7
	require.NoError(t, ValidateExecutionPayloadEnvelopeBuilderIndex(block, envelope))
	require.Error(t, ValidateExecutionPayloadEnvelopeBuilderIndex(nil, envelope))
}

func TestValidateExecutionPayloadEnvelopeCommitments(t *testing.T) {
	makePair := func(t *testing.T) (*SignedBeaconBlock, *SignedExecutionPayloadEnvelope) {
		t.Helper()
		block := NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
		block.Block.Slot = 9
		block.Block.ParentRoot = common.HexToHash("0x11")
		block.Block.Body.SyncAggregate = NewSyncAggregate()

		envelope := validTestExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
		envelope.Message.ParentBeaconBlockRoot = block.Block.ParentRoot
		envelope.Message.Payload.SlotNumber = block.Block.Slot
		envelope.Message.Payload.ParentHash = common.HexToHash("0x21")
		envelope.Message.Payload.PrevRandao = common.HexToHash("0x23")
		envelope.Message.Payload.FeeRecipient = common.HexToAddress("0x24")
		envelope.Message.Payload.GasLimit = 30_000_000
		envelope.Message.BuilderIndex = 7
		envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})

		requestsRoot, err := envelope.Message.ExecutionRequests.HashSSZ()
		require.NoError(t, err)
		requestsHash := ComputeExecutionRequestHash(GetExecutionRequestsList(&clparams.MainnetBeaconConfig, envelope.Message.ExecutionRequests))
		envelope.Message.Payload.BlockHash, err = envelope.Message.Payload.ComputeBlockHash(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
		require.NoError(t, err)
		bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
		bid.ParentBlockHash = envelope.Message.Payload.ParentHash
		bid.BlockHash = envelope.Message.Payload.BlockHash
		bid.PrevRandao = envelope.Message.Payload.PrevRandao
		bid.FeeRecipient = envelope.Message.Payload.FeeRecipient
		bid.GasLimit = envelope.Message.Payload.GasLimit
		bid.BuilderIndex = envelope.Message.BuilderIndex
		bid.ExecutionRequestsRoot = requestsRoot

		blockRoot, err := block.Block.HashSSZ()
		require.NoError(t, err)
		envelope.Message.BeaconBlockRoot = blockRoot
		return block, envelope
	}

	block, envelope := makePair(t)
	require.NoError(t, ValidateExecutionPayloadEnvelopeCommitments(&clparams.MainnetBeaconConfig, block, envelope))
	require.NoError(t, ValidateExecutionPayloadEnvelopeBidCommitments(&clparams.MainnetBeaconConfig, block, envelope))

	block, envelope = makePair(t)
	envelope.Message.Payload.GasUsed++
	require.NoError(t, ValidateExecutionPayloadEnvelopeBidCommitments(&clparams.MainnetBeaconConfig, block, envelope))
	require.Error(t, ValidateExecutionPayloadEnvelopeCommitments(&clparams.MainnetBeaconConfig, block, envelope))

	block, envelope = makePair(t)
	envelope.Message.Payload.FeeRecipient = common.HexToAddress("0x25")
	requestsHash := ComputeExecutionRequestHash(GetExecutionRequestsList(&clparams.MainnetBeaconConfig, envelope.Message.ExecutionRequests))
	var err error
	envelope.Message.Payload.BlockHash, err = envelope.Message.Payload.ComputeBlockHash(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = envelope.Message.Payload.BlockHash
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = blockRoot
	require.NotEqual(t, block.Block.Body.GetSignedExecutionPayloadBid().Message.FeeRecipient, envelope.Message.Payload.FeeRecipient)
	require.NoError(t, ValidateExecutionPayloadEnvelopeCommitments(&clparams.MainnetBeaconConfig, block, envelope))

	mutations := []struct {
		name   string
		mutate func(*SignedExecutionPayloadEnvelope)
	}{
		{"beacon block root", func(e *SignedExecutionPayloadEnvelope) { e.Message.BeaconBlockRoot[0]++ }},
		{"parent beacon block root", func(e *SignedExecutionPayloadEnvelope) { e.Message.ParentBeaconBlockRoot[0]++ }},
		{"payload slot", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.SlotNumber++ }},
		{"builder index", func(e *SignedExecutionPayloadEnvelope) { e.Message.BuilderIndex++ }},
		{"parent block hash", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.ParentHash[0]++ }},
		{"block hash", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.BlockHash[0]++ }},
		{"prev randao", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.PrevRandao[0]++ }},
		{"fee recipient without matching block hash", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.FeeRecipient[0]++ }},
		{"gas limit", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.GasLimit++ }},
		{"derived block hash", func(e *SignedExecutionPayloadEnvelope) { e.Message.Payload.GasUsed++ }},
		{"execution requests", func(e *SignedExecutionPayloadEnvelope) {
			e.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			block, envelope := makePair(t)
			mutation.mutate(envelope)
			require.Error(t, ValidateExecutionPayloadEnvelopeCommitments(&clparams.MainnetBeaconConfig, block, envelope))
		})
	}
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

	progressiveTransactions := validTestExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	progressiveTransactions.Message.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}})
	require.NoError(t, progressiveTransactions.ValidateForConfig(&cfg))
	require.NoError(t, progressiveTransactions.ValidateForPersistence(&cfg))

	for _, test := range []struct {
		name   string
		mutate func(*SignedExecutionPayloadEnvelope)
	}{
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
	_, err := envelope.EncodeSSZ(nil)
	require.ErrorContains(t, err, "exceeds max")
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
	require.ErrorIs(t, err, ssz.ErrTooBigList, err)
	require.Same(t, message, target.Message)
}

func TestSignedExecutionPayloadBidStrictDecodeInitializesMessage(t *testing.T) {
	source := &SignedExecutionPayloadBid{Message: executionPayloadBidWithCommitments(1)}
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)

	var decoded SignedExecutionPayloadBid
	require.NoError(t, decoded.DecodeSSZStrict(encoded, int(clparams.GloasVersion)))
	require.NotNil(t, decoded.Message)
	require.Equal(t, 1, decoded.Message.BlobKzgCommitments.Len())
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

func TestExecutionPayloadEnvelopeHashSSZRejectsIncompleteEnvelope(t *testing.T) {
	_, errNilMessage := (&SignedExecutionPayloadEnvelope{}).HashSSZ()
	require.Error(t, errNilMessage)
	_, errNilPayload := (&SignedExecutionPayloadEnvelope{
		Message: &ExecutionPayloadEnvelope{ExecutionRequests: &ExecutionRequests{}},
	}).HashSSZ()
	require.Error(t, errNilPayload)
	_, errNilRequests := (&SignedExecutionPayloadEnvelope{
		Message: &ExecutionPayloadEnvelope{Payload: &Eth1Block{}},
	}).HashSSZ()
	require.Error(t, errNilRequests)
}

func TestIndexedPayloadAttestationPreservesLimit(t *testing.T) {
	for _, limit := range []int{16, 32, 512} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.PtcSize = uint64(limit)
			source := NewIndexedPayloadAttestation(&cfg)
			require.Equal(t, limit, source.AttestingIndices.Cap())
			for _, index := range []uint64{1, 2, 3} {
				source.AttestingIndices.Append(index)
			}
			source.Data.Slot = 42
			source.Signature[0] = 9
			roots := map[int]string{
				16:  "ed114baf42aac42d5c115ed017862e26138544d8e8fbd9b58466da9dfa0b2f55",
				32:  "eac541ed75add596f34e7d491f512397ad78e73126db30505ac611ea7eeca09c",
				512: "f273bb09e1c6e64f67d52646a780c2a19a2ef6621e724b4ed7982ca505c9bada",
			}
			listRoot, err := source.AttestingIndices.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(roots[limit]), common.Hash(listRoot))
			expected, err := source.HashSSZ()
			require.NoError(t, err)
			encoded, err := source.EncodeSSZ(nil)
			require.NoError(t, err)
			t.Run("decode", func(t *testing.T) {
				cfg := clparams.MainnetBeaconConfig
				cfg.PtcSize = uint64(limit)
				decoded := NewIndexedPayloadAttestation(&cfg)
				require.Equal(t, limit, decoded.AttestingIndices.Cap())
				require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
				root, err := decoded.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, expected, root)
				require.Equal(t, limit, decoded.AttestingIndices.Cap())
			})
			t.Run("clone", func(t *testing.T) {
				cloned := source.Clone().(*IndexedPayloadAttestation)
				root, err := cloned.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, expected, root)
				require.Equal(t, limit, cloned.AttestingIndices.Cap())
				cloned.AttestingIndices.Set(0, 99)
				cloned.Data.Slot++
				require.Equal(t, uint64(1), source.AttestingIndices.Get(0))
				require.Equal(t, uint64(42), source.Data.Slot)
			})
		})
	}
}

func TestIndexedPayloadAttestationDecodeLimit(t *testing.T) {
	for _, limit := range []int{16, 32, 512} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			for _, size := range []int{0, limit, limit + 1} {
				source := NewIndexedPayloadAttestation(nil)
				source.AttestingIndices = solid.NewRawUint64List(limit+1, make([]uint64, size))
				encoded, err := source.EncodeSSZ(nil)
				require.NoError(t, err)
				cfg := clparams.MainnetBeaconConfig
				cfg.PtcSize = uint64(limit)
				decoded := NewIndexedPayloadAttestation(&cfg)
				require.Equal(t, limit, decoded.AttestingIndices.Cap())
				err = decoded.DecodeSSZ(encoded, int(clparams.GloasVersion))
				if size > limit {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestIndexedPayloadAttestationDecodeMainnetDefault(t *testing.T) {
	source := NewIndexedPayloadAttestation(nil)
	source.AttestingIndices.Append(1)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	var decoded IndexedPayloadAttestation
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, int(clparams.MainnetBeaconConfig.PtcSize), decoded.AttestingIndices.Cap())
	expected, err := source.HashSSZ()
	require.NoError(t, err)
	root, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationEmptyConfig(t *testing.T) {
	att := NewIndexedPayloadAttestation(&clparams.BeaconChainConfig{})
	require.Equal(t, int(clparams.MaxPtcSize), att.AttestingIndices.Cap())
	root, err := att.HashSSZ()
	require.NoError(t, err)
	expected, err := NewIndexedPayloadAttestation(nil).HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationDecodeZeroLimit(t *testing.T) {
	source := NewIndexedPayloadAttestation(nil)
	source.AttestingIndices.Append(1)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	expected, err := source.HashSSZ()
	require.NoError(t, err)
	decoded := &IndexedPayloadAttestation{AttestingIndices: solid.NewRawUint64List(0, nil)}
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, int(clparams.MaxPtcSize), decoded.AttestingIndices.Cap())
	root, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationCloneZeroLimit(t *testing.T) {
	source := IndexedPayloadAttestation{AttestingIndices: solid.NewRawUint64List(0, []uint64{1}), Data: &PayloadAttestationData{}}
	require.Equal(t, 0, source.AttestingIndices.Cap())
	cloned := source.Clone().(*IndexedPayloadAttestation)
	require.Equal(t, int(clparams.MaxPtcSize), cloned.AttestingIndices.Cap())
	expected := NewIndexedPayloadAttestation(nil)
	expected.AttestingIndices.Append(1)
	expectedRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	root, err := cloned.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, root)
	require.Equal(t, 0, source.AttestingIndices.Cap())
}
