// WitnessBlockTest extends BlockTest with EIP-8025 execution witness expectations.
// It parses the executionWitness field from each block in the JSON fixture and
// provides access to expected state nodes, bytecodes, and headers for comparison
// against the output of debug_executionWitness.
package testutil

import (
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common/hexutil"
)

// WitnessBlockTest wraps a standard BlockTest with per-block witness expectations.
type WitnessBlockTest struct {
	BlockTest
	witnessData witnessTestJSON
}

// witnessTestJSON mirrors the top-level test JSON but only captures
// the executionWitness field from each block.
type witnessTestJSON struct {
	Blocks []witnessTestBlock `json:"blocks"`
}

type witnessTestBlock struct {
	ExecutionWitness *ExpectedWitness `json:"executionWitness"`
}

// ExpectedWitness holds the expected execution witness arrays from a fixture.
type ExpectedWitness struct {
	State   []hexutil.Bytes `json:"state"`
	Codes   []hexutil.Bytes `json:"codes"`
	Headers []hexutil.Bytes `json:"headers"`
}

// UnmarshalJSON deserializes a test fixture into both the embedded BlockTest
// (standard blockchain test fields) and the witness-specific block data.
func (wbt *WitnessBlockTest) UnmarshalJSON(in []byte) error {
	if err := wbt.BlockTest.UnmarshalJSON(in); err != nil {
		return err
	}
	return jsoniter.ConfigFastest.Unmarshal(in, &wbt.witnessData)
}

// ExpectedWitnessForBlock returns the expected witness for the given block index,
// or nil if the block has no executionWitness field.
func (wbt *WitnessBlockTest) ExpectedWitnessForBlock(blockIndex int) *ExpectedWitness {
	if blockIndex < 0 || blockIndex >= len(wbt.witnessData.Blocks) {
		return nil
	}
	return wbt.witnessData.Blocks[blockIndex].ExecutionWitness
}

// NumBlocks returns the total number of blocks in the test fixture.
func (wbt *WitnessBlockTest) NumBlocks() int {
	return len(wbt.witnessData.Blocks)
}
