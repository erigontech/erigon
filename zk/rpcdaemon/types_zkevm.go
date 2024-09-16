package types

import (
	"github.com/gateway-fm/cdk-erigon-lib/common"
)

// Batch structure
type Batch struct {
	Number              ArgUint64      `json:"number"`
	ForcedBatchNumber   *ArgUint64     `json:"forcedBatchNumber,omitempty"`
	Coinbase            common.Address `json:"coinbase"`
	StateRoot           common.Hash    `json:"stateRoot"`
	GlobalExitRoot      common.Hash    `json:"globalExitRoot"`
	MainnetExitRoot     common.Hash    `json:"mainnetExitRoot"`
	RollupExitRoot      common.Hash    `json:"rollupExitRoot"`
	LocalExitRoot       common.Hash    `json:"localExitRoot"`
	AccInputHash        common.Hash    `json:"accInputHash"`
	Timestamp           ArgUint64      `json:"timestamp"`
	SendSequencesTxHash *common.Hash   `json:"sendSequencesTxHash"`
	VerifyBatchTxHash   *common.Hash   `json:"verifyBatchTxHash"`
	Closed              bool           `json:"closed"`
	Blocks              []interface{}  `json:"blocks"`
	Transactions        []interface{}  `json:"transactions"`
	BatchL2Data         ArgBytes       `json:"batchL2Data"`
}

type BatchDataSlim struct {
	Number      ArgUint64 `json:"number"`
	BatchL2Data ArgBytes  `json:"batchL2Data,omitempty"`
	Empty       bool      `json:"empty"`
}

type BlockWithInfoRootAndGer struct {
	*Block
	BlockInfoRoot  common.Hash `json:"blockInfoRoot"`
	GlobalExitRoot common.Hash `json:"globalExitRoot"`
}
