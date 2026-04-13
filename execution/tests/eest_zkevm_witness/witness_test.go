// Package eest_zkevm_witness_test validates Erigon's execution witness generation
// (debug_executionWitness) against the 93 EIP-8025 fixtures from the ethereum/execution-spec-tests
// zkevm@v0.3.3 release. Each fixture contains expected witness arrays (state MPT nodes,
// bytecodes, block headers) that are compared element-by-element against Erigon's output.
package eest_zkevm_witness_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestExecutionSpecWitness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping witness tests in short mode")
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	// Enable historical commitment so witness generation works for historical blocks.
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	dir := filepath.Join("..", "execution-spec-tests", "blockchain_tests_zkevm")
	bt := new(testutil.TestMatcher)

	// All 93 witness tests currently fail due to known limitations in Erigon's
	// witness generation (debug_execution_witness.go). Root causes (Task 5 triage):
	//   1. State node ordering differs from reference implementation (52% of tests)
	//   2. Extra state nodes included — superset of expected (48% of tests)
	//   3. Codes ordering differs — keccak256 sort vs access-order (99% of tests)
	//   4. Missing parent block headers (99% of tests)
	// Tracked by: https://github.com/erigontech/erigon/issues/20442
	bt.Fails(".", "witness generation mismatch (#20442): state ordering/extra nodes, codes ordering, missing headers")

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.WitnessBlockTest) {
		// Amsterdam fixtures require experimental block access list support.
		test.ExperimentalBAL = true

		// Run the standard blockchain test: insert blocks, validate post-state.
		// Block execution should always succeed — Fatal on failure.
		if err := test.Run(t); err != nil {
			t.Fatalf("block execution failed: %v", err)
		}

		if test.M == nil {
			t.Fatal("ExecModuleTester not set after BlockTest.Run")
		}

		// Set up the debug API using the test's ExecModuleTester.
		base := jsonrpc.NewBaseApi(
			nil,
			test.M.StateCache,
			test.M.BlockReader,
			false,
			rpccfg.DefaultEvmCallTimeout,
			test.M.Engine,
			test.M.Dirs,
			nil, 0, 0,
		)
		api := jsonrpc.NewPrivateDebugAPI(base, test.M.DB, nil, 0, false)
		ctx := context.Background()

		// Compare witness for each block that has expected witness data.
		// Collect the first mismatch as an error for CheckFailure.
		var witnessErr error
		for i := 0; i < test.NumBlocks(); i++ {
			expected := test.ExpectedWitnessForBlock(i)
			if expected == nil {
				continue
			}

			blockNum := uint64(i + 1) // genesis is block 0, first test block is 1
			bn := rpc.BlockNumber(blockNum)
			result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn})
			if err != nil {
				witnessErr = fmt.Errorf("ExecutionWitness failed for block %d: %w", blockNum, err)
				break
			}
			if result == nil {
				witnessErr = fmt.Errorf("ExecutionWitness returned nil for block %d", blockNum)
				break
			}

			if err := compareWitness(t, blockNum, expected, result); err != nil {
				witnessErr = err
				break
			}
		}

		// Route witness comparison result through CheckFailure so bt.Fails
		// patterns can mark known mismatches as expected.
		if err := bt.CheckFailure(t, witnessErr); err != nil {
			t.Error(err)
		}
	})
}

// compareWitness performs exact ordered comparison of witness arrays.
// Returns an error describing the first mismatch, or nil if all match.
// Logs set-diff diagnostics on mismatch to help distinguish ordering vs content issues.
func compareWitness(t *testing.T, blockNum uint64, expected *testutil.ExpectedWitness, actual *jsonrpc.ExecutionWitnessResult) error {
	t.Helper()

	if err := compareByteSlices(blockNum, "State", expected.State, actual.State); err != nil {
		reportSetDiff(t, blockNum, "State", expected.State, actual.State)
		return err
	}
	if err := compareByteSlices(blockNum, "Codes", expected.Codes, actual.Codes); err != nil {
		reportSetDiff(t, blockNum, "Codes", expected.Codes, actual.Codes)
		return err
	}
	if err := compareByteSlices(blockNum, "Headers", expected.Headers, actual.Headers); err != nil {
		reportSetDiff(t, blockNum, "Headers", expected.Headers, actual.Headers)
		return err
	}
	return nil
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
