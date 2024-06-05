package l1infotree

import (
	"github.com/gateway-fm/cdk-erigon-lib/common"
)

// L1InfoTree holds the test vector for the merkle tree
type L1InfoTree struct {
	PreviousLeafValues []common.Hash `json:"previousLeafValues"`
	CurrentRoot        common.Hash   `json:"currentRoot"`
	NewLeafValue       common.Hash   `json:"newLeafValue"`
	NewRoot            common.Hash   `json:"newRoot"`
}

// L1InfoTree holds the test vector for the merkle tree
type L1InfoTreeProof struct {
	Leaves []common.Hash `json:"leaves"`
	Index  uint          `json:"index"`
	Proof  []common.Hash `json:"proof"`
	Root   common.Hash   `json:"root"`
}
