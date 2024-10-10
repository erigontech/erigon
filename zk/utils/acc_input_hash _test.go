package utils

import (
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func Test_CalculateEtrogAccInputHash(t *testing.T) {
	testCases := []struct {
		oldAccInputHash      string
		batchTransactionData string
		l1InfoRoot           string
		limitTimestamp       uint64
		sequencerAddress     string
		forcedBlockHashL1    string
		Expected             string
	}{
		{
			oldAccInputHash:      "0x0000000000000000000000000000000000000000000000000000000000000000",
			batchTransactionData: "0x0b73e6af6e00000001ee80843b9aca00830186a0944d5cf5032b2a844602278b01199ed191a86c93ff88016345785d8a0000808203e880801cee7e01dc62f69a12c3510c6d64de04ee6346d84b6a017f3e786c7d87f963e75d8cc91fa983cd6d9cf55fff80d73bd26cd333b0f098acc1e58edb1fd484ad731bff0b0000000100000002",
			l1InfoRoot:           "0x462ed3d694d640f04f637e5e3893e8d12f407a53f50201401fd992bb5ab0faf0",
			limitTimestamp:       1944498031,
			sequencerAddress:     "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
			forcedBlockHashL1:    "0x0000000000000000000000000000000000000000000000000000000000000000",
			Expected:             "0xcfae2cfa3b8f3f12abce1bccd90e9b203dfdbe56c0c412114f2d3e67c9a897db",
		},
	}

	for _, tc := range testCases {
		oldAccInputHash := common.HexToHash(tc.oldAccInputHash)
		batchTransactionData := common.FromHex(tc.batchTransactionData)
		l1InfoRoot := common.HexToHash(tc.l1InfoRoot)
		sequencerAddress := common.HexToAddress(tc.sequencerAddress)
		forcedBlockHashL1 := common.HexToHash(tc.forcedBlockHashL1)

		newAccInputHash := CalculateEtrogAccInputHash(
			oldAccInputHash,
			batchTransactionData,
			l1InfoRoot,
			tc.limitTimestamp,
			sequencerAddress,
			forcedBlockHashL1,
		)

		require.Equal(t, common.HexToHash(tc.Expected), *newAccInputHash)
	}
}

func Test_CalculatePreEtrogAccInputHash(t *testing.T) {
	testCases := []struct {
		oldAccInputHash      string
		batchTransactionData string
		globalExitRoot       string
		timestamp            uint64
		sequencerAddress     string
		Expected             string
	}{
		{
			oldAccInputHash:      "0x0000000000000000000000000000000000000000000000000000000000000000",
			batchTransactionData: "0xee80843b9aca00830186a0944d5cf5032b2a844602278b01199ed191a86c93ff88016345785d8a0000808203e880801cee7e01dc62f69a12c3510c6d64de04ee6346d84b6a017f3e786c7d87f963e75d8cc91fa983cd6d9cf55fff80d73bd26cd333b0f098acc1e58edb1fd484ad731b",
			globalExitRoot:       "0x090bcaf734c4f06c93954a827b45a6e8c67b8e0fd1e0a35a1c5982d6961828f9",
			timestamp:            1944498031,
			sequencerAddress:     "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D",
			Expected:             "0x704d5cfd3e44b82028f7f8cae31168267a7422c5a447b90a65134116da5a8432",
		},
	}

	for _, tc := range testCases {
		oldAccInputHash := common.HexToHash(tc.oldAccInputHash)
		batchTransactionData := common.FromHex(tc.batchTransactionData)
		globalExitRoot := common.HexToHash(tc.globalExitRoot)
		sequencerAddress := common.HexToAddress(tc.sequencerAddress)

		newAccInputHash := CalculatePreEtrogAccInputHash(
			oldAccInputHash,
			batchTransactionData,
			globalExitRoot,
			tc.timestamp,
			sequencerAddress,
		)

		require.Equal(t, common.HexToHash(tc.Expected), *newAccInputHash)
	}
}

func Test_CalculateBatchHashData(t *testing.T) {
	testCases := []struct {
		batchL2Data string
		Expected    string
	}{
		{
			batchL2Data: "0x0b73e6af6e00000001ee80843b9aca00830186a0944d5cf5032b2a844602278b01199ed191a86c93ff88016345785d8a0000808203e880801cee7e01dc62f69a12c3510c6d64de04ee6346d84b6a017f3e786c7d87f963e75d8cc91fa983cd6d9cf55fff80d73bd26cd333b0f098acc1e58edb1fd484ad731bff0b0000000100000002",
			Expected:    "0x5e7875ab198c4d93379c92990a5d0111af59a0e62b2c4a0e3898e5bd24a18e58",
		},
	}

	for _, tc := range testCases {
		data := common.FromHex(tc.batchL2Data)
		batchHash := CalculateBatchHashData(data)

		require.Equal(t, common.FromHex(tc.Expected), batchHash)
	}
}
