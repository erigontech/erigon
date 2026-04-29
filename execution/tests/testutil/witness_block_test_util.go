// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
