package execctx_test

import (
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func testCommitmentStateKey() []byte {
	if statecfg.ExperimentalCommitmentV3 {
		return commitment.KeyCommitmentV3State
	}
	return commitment.KeyCommitmentState
}

func testDecodeCommitmentState(v []byte) (txNum, blockNum uint64, root []byte, err error) {
	if statecfg.ExperimentalCommitmentV3 {
		blockNum, txNum, root, err = commitment.DecodeCommitmentV3State(v)
		return txNum, blockNum, root, err
	}
	root, blockNum, txNum, err = commitment.HexTrieExtractStateRoot(v)
	return txNum, blockNum, root, err
}
