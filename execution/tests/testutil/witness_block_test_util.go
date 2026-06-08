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
	"strconv"

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
	witnesses    []*ExpectedWitness
	blockNumbers []string
	exceptions   []string
}

type witnessBlockJSON struct {
	BlockNumber      string           `json:"blocknumber"`
	ExpectException  string           `json:"expectException"`
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
	wbt.blockNumbers = make([]string, len(wj.Blocks))
	wbt.exceptions = make([]string, len(wj.Blocks))
	for i, b := range wj.Blocks {
		wbt.witnesses[i] = b.ExecutionWitness
		wbt.blockNumbers[i] = b.BlockNumber
		wbt.exceptions[i] = b.ExpectException
	}
	return nil
}

func (wbt *WitnessBlockTest) NumBlocks() int {
	return len(wbt.witnesses)
}

// BlockNumberForBlock returns the canonical block number for block index i,
// parsed from the fixture's "blocknumber" field, or (0, false) when absent,
// unparseable, or i is out of range.
func (wbt *WitnessBlockTest) BlockNumberForBlock(i int) (uint64, bool) {
	if i < 0 || i >= len(wbt.blockNumbers) {
		return 0, false
	}
	n, err := strconv.ParseUint(wbt.blockNumbers[i], 0, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// BlockExpectsException reports whether block index i is a fixture block that
// is expected to be rejected during import. Such blocks carry an
// executionWitness (the stateless-verifier input) but no canonical block
// number, so debug_executionWitness cannot be queried for them.
func (wbt *WitnessBlockTest) BlockExpectsException(i int) bool {
	if i < 0 || i >= len(wbt.exceptions) {
		return false
	}
	return wbt.exceptions[i] != ""
}

// ExpectedWitnessForBlock returns the expected witness for block index i, or
// nil when the block has no executionWitness or i is out of range.
func (wbt *WitnessBlockTest) ExpectedWitnessForBlock(i int) *ExpectedWitness {
	if i < 0 || i >= len(wbt.witnesses) {
		return nil
	}
	return wbt.witnesses[i]
}
