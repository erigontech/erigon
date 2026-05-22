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

// Package eest_zkevm_witness_test validates Erigon's execution witness generation
// (debug_executionWitness) against the EIP-8025 fixtures from
// ethereum/execution-spec-tests. The exact fixture version is pinned via
// test-fixtures.json (key eest_zkevm). Each fixture contains expected witness
// arrays (state MPT nodes, bytecodes, block headers) that are compared
// element-by-element against Erigon's output.
package eest_zkevm_witness_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestExecutionSpecWitness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping witness tests in short mode")
	}

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	// Enable historical commitment so witness generation works for historical blocks.
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	// Fixtures are downloaded + sha256-verified + extracted by the lazy-download
	// manifest machinery introduced in #21002: `make test-fixtures-zkevm` populates
	// test-fixtures-cache/eest_zkevm/ (preserving the tarball's top-level
	// `fixtures/blockchain_tests/...` layout). This replaces the prior submodule/LFS
	// path under execution/tests/execution-spec-tests/.
	dir := filepath.Join("..", "..", "..", "test-fixtures-cache", "eest_zkevm", "fixtures", "blockchain_tests")
	bt := new(testutil.TestMatcher)
	bt.NoParallel = true

	// Run the full corpus serially. The harness spins up fresh MDBX/state
	// machinery per file, and we want the CI signal to reflect the fixture
	// itself rather than parallel memory pressure.
	// All 93 fixtures fail on State node ordering and/or Codes ordering mismatches
	// (tracked by #20442). Headers field was fixed by including the parent header
	// and sorting ascending (#20534), but no fixture passes yet because every one
	// also has State/Codes issues. The three remaining root causes are:
	//   1. State MPT nodes emitted in wrong order (~60% ordering-only, ~40% extra nodes)
	//   2. Bytecodes emitted in wrong order (nearly 100% ordering-only)
	//   3. 8 multi-block BLOCKHASH fixtures still have Headers range mismatches
	bt.Fails(".", "witness State/Codes ordering mismatch (#20442, #20534): state nodes and bytecodes emitted in wrong order")

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.WitnessBlockTest) {
		// Amsterdam fixtures require experimental block access list support.
		test.ExperimentalBAL = true

		// Run the standard blockchain test: insert blocks, validate post-state.
		// Block-execution failures are routed through bt.CheckFailure so the
		// suite-wide bt.Fails(...) absorbs EIP-implementation gaps the same way
		// it absorbs documented witness-comparison gaps below. The returned
		// tester's lifetime is bound to t via t.Cleanup; do NOT close it here.
		m, err := test.RunWithTester(t)
		if err != nil {
			if cferr := bt.CheckFailure(t, fmt.Errorf("block execution failed: %w", err)); cferr != nil {
				t.Error(cferr)
			}
			return
		}

		// Set up the debug API using the returned ExecModuleTester.
		base := jsonrpc.NewBaseApi(
			nil,
			m.StateCache,
			m.BlockReader,
			false,
			rpccfg.DefaultEvmCallTimeout,
			m.Engine,
			m.Dirs,
			nil, 0, 0,
		)
		api := jsonrpc.NewPrivateDebugAPI(base, m.DB, nil, 0, false)
		ctx := context.Background()

		// debug_executionWitness now reads the commitment-history flag from the
		// DB (set by node/eth/backend on real startups). The test framework
		// builds its DB from scratch, so write the flag explicitly here.
		if err := m.DB.Update(ctx, func(tx kv.RwTx) error {
			return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
		}); err != nil {
			t.Fatalf("write commitment-history flag: %v", err)
		}

		// Compare witness for each block that has expected witness data.
		// RPC infrastructure errors (endpoint failure, nil result) are fatal —
		// they indicate a regression in the RPC layer, not a known witness mismatch.
		// Comparison mismatches are collected across all blocks and routed through
		// CheckFailure so bt.Fails patterns can mark known issues as expected.
		var witnessErrs []error
		for i := 0; i < test.NumBlocks(); i++ {
			expected := test.ExpectedWitnessForBlock(i)
			if expected == nil {
				continue
			}

			blockNum := uint64(i + 1) // genesis is block 0, first test block is 1
			bn := rpc.BlockNumber(blockNum)
			result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn})
			if err != nil {
				t.Fatalf("ExecutionWitness RPC failed for block %d: %v", blockNum, err)
			}
			if result == nil {
				t.Fatalf("ExecutionWitness returned nil for block %d", blockNum)
			}

			if err := compareWitness(t, blockNum, expected, result); err != nil {
				witnessErrs = append(witnessErrs, err)
			}
		}

		// Route witness comparison result through CheckFailure so bt.Fails
		// patterns can mark known mismatches as expected.
		if err := bt.CheckFailure(t, errors.Join(witnessErrs...)); err != nil {
			t.Error(err)
		}
	})
}

// compareWitness performs exact ordered comparison of witness arrays.
// Returns a joined error describing all field mismatches, or nil if all match.
// Logs set-diff diagnostics on mismatch to help distinguish ordering vs content issues.
func compareWitness(t *testing.T, blockNum uint64, expected *testutil.ExpectedWitness, actual *jsonrpc.ExecutionWitnessResult) error {
	t.Helper()

	var fieldErrs []error
	if err := compareByteSlices(blockNum, "State", expected.State, actual.State); err != nil {
		reportSetDiff(t, blockNum, "State", expected.State, actual.State)
		fieldErrs = append(fieldErrs, err)
	}
	if err := compareByteSlices(blockNum, "Codes", expected.Codes, actual.Codes); err != nil {
		reportSetDiff(t, blockNum, "Codes", expected.Codes, actual.Codes)
		fieldErrs = append(fieldErrs, err)
	}
	actualHeaders, err := jsonHeadersToRLP(actual.Headers)
	if err != nil {
		fieldErrs = append(fieldErrs, fmt.Errorf("block %d Headers: re-encode RPC headers for comparison: %w", blockNum, err))
	} else if err := compareByteSlices(blockNum, "Headers", expected.Headers, actualHeaders); err != nil {
		reportSetDiff(t, blockNum, "Headers", expected.Headers, actualHeaders)
		fieldErrs = append(fieldErrs, err)
	}
	return errors.Join(fieldErrs...)
}

// jsonHeadersToRLP converts JSON-object headers returned by debug_executionWitness
// (post PR #21224, which aligned the wire format with Geth) back into RLP-encoded
// bytes so they can be compared against the EEST fixture format (`hexutil.Bytes`).
// The reverse path mirrors marshalWitnessHeader: rename "balHash" back to
// "blockAccessListHash", drop fields not present on types.Header ("hash", "size"),
// then JSON-unmarshal into types.Header and RLP-encode.
func jsonHeadersToRLP(maps []map[string]any) ([]hexutil.Bytes, error) {
	out := make([]hexutil.Bytes, 0, len(maps))
	for i, m := range maps {
		normalized := make(map[string]any, len(m))
		for k, v := range m {
			switch k {
			case "balHash":
				normalized["blockAccessListHash"] = v
			case "hash", "size":
				// Not part of types.Header's JSON schema.
			default:
				normalized[k] = v
			}
		}
		jsonBytes, err := json.Marshal(normalized)
		if err != nil {
			return nil, fmt.Errorf("header[%d]: marshal map to JSON: %w", i, err)
		}
		var h types.Header
		if err := json.Unmarshal(jsonBytes, &h); err != nil {
			return nil, fmt.Errorf("header[%d]: unmarshal JSON to types.Header: %w", i, err)
		}
		rlpBytes, err := rlp.EncodeToBytes(&h)
		if err != nil {
			return nil, fmt.Errorf("header[%d]: RLP encode: %w", i, err)
		}
		out = append(out, rlpBytes)
	}
	return out, nil
}

// compareByteSlices compares two slices of hexutil.Bytes element-by-element.
// Returns an error on the first mismatch, or nil if identical.
func compareByteSlices(blockNum uint64, field string, expected, actual []hexutil.Bytes) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("block %d %s: length mismatch: expected %d elements, got %d",
			blockNum, field, len(expected), len(actual))
	}
	for i := range expected {
		if !bytes.Equal(expected[i], actual[i]) {
			return fmt.Errorf("block %d %s[%d]: mismatch expected=%s actual=%s",
				blockNum, field, i,
				truncHex(expected[i], 32),
				truncHex(actual[i], 32))
		}
	}
	return nil
}

// reportSetDiff compares two slices as unordered sets and reports only elements
// present in one but not the other. Helps distinguish ordering-only mismatches
// from genuine content differences.
func reportSetDiff(t *testing.T, blockNum uint64, field string, expected, actual []hexutil.Bytes) {
	t.Helper()
	expectedSet := make(map[string]int)
	actualSet := make(map[string]int)
	for _, e := range expected {
		expectedSet[hex.EncodeToString(e)]++
	}
	for _, a := range actual {
		actualSet[hex.EncodeToString(a)]++
	}
	var onlyInExpected, onlyInActual int
	for k, cnt := range expectedSet {
		if actualSet[k] < cnt {
			onlyInExpected += cnt - actualSet[k]
		}
	}
	for k, cnt := range actualSet {
		if expectedSet[k] < cnt {
			onlyInActual += cnt - expectedSet[k]
		}
	}
	if onlyInExpected == 0 && onlyInActual == 0 {
		t.Logf("block %d %s: SET-EQUAL (same %d elements, different order only)", blockNum, field, len(expected))
	} else {
		t.Logf("block %d %s: SET-DIFF (expected has %d unique elements not in actual, actual has %d unique elements not in expected)",
			blockNum, field, onlyInExpected, onlyInActual)
	}
}

// truncHex returns a hex string of b, truncated to maxBytes with "..." suffix.
func truncHex(b []byte, maxBytes int) string {
	if len(b) <= maxBytes {
		return fmt.Sprintf("0x%s (%d bytes)", hex.EncodeToString(b), len(b))
	}
	return fmt.Sprintf("0x%s... (%d bytes)", hex.EncodeToString(b[:maxBytes]), len(b))
}
