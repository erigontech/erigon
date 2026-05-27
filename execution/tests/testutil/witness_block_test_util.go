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

package testutil

import (
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common/hexutil"
)

// ExpectedWitness is the per-block executionWitness as stored in the EEST
// zkevm fixtures: three arrays of RLP-hex strings, no keys field.
type ExpectedWitness struct {
	State   []hexutil.Bytes `json:"state"`
	Codes   []hexutil.Bytes `json:"codes"`
	Headers []hexutil.Bytes `json:"headers"`
}

// WitnessBlockTest is a BlockTest that additionally captures the per-block
// executionWitness data so the witness runner can compare it against the RPC.
type WitnessBlockTest struct {
	BlockTest
	witnesses []*ExpectedWitness
}

type witnessBlockJSON struct {
	ExecutionWitness *ExpectedWitness `json:"executionWitness"`
}

type witnessJSON struct {
	Blocks []witnessBlockJSON `json:"blocks"`
}

// UnmarshalJSON parses the standard block-test object and then the parallel
// per-block executionWitness data.
func (wbt *WitnessBlockTest) UnmarshalJSON(in []byte) error {
	if err := wbt.BlockTest.UnmarshalJSON(in); err != nil {
		return err
	}
	var wj witnessJSON
	if err := jsoniter.ConfigFastest.Unmarshal(in, &wj); err != nil {
		return err
	}
	wbt.witnesses = make([]*ExpectedWitness, len(wj.Blocks))
	for i, b := range wj.Blocks {
		wbt.witnesses[i] = b.ExecutionWitness
	}
	return nil
}

func (wbt *WitnessBlockTest) NumBlocks() int {
	return len(wbt.witnesses)
}

// ExpectedWitnessForBlock returns the expected witness for block index i, or
// nil when the block has no executionWitness or i is out of range.
func (wbt *WitnessBlockTest) ExpectedWitnessForBlock(i int) *ExpectedWitness {
	if i < 0 || i >= len(wbt.witnesses) {
		return nil
	}
	return wbt.witnesses[i]
}
