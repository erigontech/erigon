package contracts

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/stretchr/testify/require"
)

func TestZkContractsEventsHash(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		eventSig     string
		expectedHash common.Hash
	}{
		{
			name:         "SequenceBatches_PreEtrog",
			eventSig:     "SequenceBatches(uint64)",
			expectedHash: SequenceBatchesTopicPreEtrog,
		},
		{
			name:         "SequenceBatches_Etrog",
			eventSig:     "SequenceBatches(uint64,bytes32)",
			expectedHash: SequenceBatchesTopicEtrog,
		},
		{
			name:         "VerifyBatches_Validium_Etrog",
			eventSig:     "VerifyBatches(uint64,bytes32,address)",
			expectedHash: VerificationValidiumTopicEtrog,
		},
		{
			name:         "VerifyBatches_PreEtrog",
			eventSig:     "VerifyBatchesTrustedAggregator(uint64,bytes32,address)",
			expectedHash: VerificationTopicPreEtrog,
		},
		{
			name:         "VerifyBatches_Etrog",
			eventSig:     "VerifyBatchesTrustedAggregator(uint32,uint64,bytes32,bytes32,address)",
			expectedHash: VerificationTopicEtrog,
		},
		{
			name:         "UpdateL1InfoTree",
			eventSig:     "UpdateL1InfoTree(bytes32,bytes32)",
			expectedHash: UpdateL1InfoTreeTopic,
		},
		{
			name:         "UpdateL1InfoTreeV2",
			eventSig:     "UpdateL1InfoTreeV2(bytes32,uint32,uint256,uint64)",
			expectedHash: UpdateL1InfoTreeV2Topic,
		},
		{
			name:         "InitialSequenceBatches",
			eventSig:     "InitialSequenceBatches(bytes,bytes32,address)",
			expectedHash: InitialSequenceBatchesTopic,
		},
		{
			name:         "AddNewRollupTypeTopic_PreBanana",
			eventSig:     "AddNewRollupType(uint32,address,address,uint64,uint8,bytes32,string)",
			expectedHash: AddNewRollupTypeTopic,
		},
		{
			name:         "AddNewRollupTypeTopic_Banana",
			eventSig:     "AddNewRollupType(uint32,address,address,uint64,uint8,bytes32,string,bytes32)",
			expectedHash: AddNewRollupTypeTopicBanana,
		},
		{
			name:         "CreateNewRollup",
			eventSig:     "CreateNewRollup(uint32,uint32,address,uint64,address)",
			expectedHash: CreateNewRollupTopic,
		},
		{
			name:         "UpdateRollup",
			eventSig:     "UpdateRollup(uint32,uint32,uint64)",
			expectedHash: UpdateRollupTopic,
		},
		{
			name:         "RollbackBatches",
			eventSig:     "RollbackBatches(uint64,bytes32)",
			expectedHash: RollbackBatchesTopic,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actualHash := crypto.Keccak256Hash([]byte(c.eventSig))
			require.Equal(t, c.expectedHash, actualHash)
		})
	}
}
