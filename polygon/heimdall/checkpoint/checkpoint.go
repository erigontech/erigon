package checkpoint

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// Checkpoint defines a response object type of bor checkpoint
type Checkpoint struct {
	Proposer   libcommon.Address `json:"proposer"`
	StartBlock *big.Int          `json:"start_block"`
	EndBlock   *big.Int          `json:"end_block"`
	RootHash   libcommon.Hash    `json:"root_hash"`
	BorChainID string            `json:"bor_chain_id"`
	Timestamp  uint64            `json:"timestamp"`
}

func (m Checkpoint) String() string {
	return fmt.Sprintf(
		"Checkpoint {%v (%d:%d) %v %v %v}",
		m.Proposer.String(),
		m.StartBlock,
		m.EndBlock,
		m.RootHash.Hex(),
		m.BorChainID,
		m.Timestamp,
	)
}

type CheckpointResponse struct {
	Height string     `json:"height"`
	Result Checkpoint `json:"result"`
}

type CheckpointCount struct {
	Result int64 `json:"result"`
}

type CheckpointCountResponse struct {
	Height string          `json:"height"`
	Result CheckpointCount `json:"result"`
}
