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

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.WitnessBlockTest) {
		// Amsterdam fixtures require experimental block access list support.
		test.ExperimentalBAL = true

		// Run the standard blockchain test: insert blocks, validate post-state.
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
			return
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
		for i := 0; i < test.NumBlocks(); i++ {
			expected := test.ExpectedWitnessForBlock(i)
			if expected == nil {
				continue
			}

			blockNum := uint64(i + 1) // genesis is block 0, first test block is 1
			bn := rpc.BlockNumber(blockNum)
			result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn})
			if err != nil {
				t.Errorf("ExecutionWitness failed for block %d: %v", blockNum, err)
				continue
			}
			if result == nil {
				t.Errorf("ExecutionWitness returned nil for block %d", blockNum)
				continue
			}

			compareWitness(t, blockNum, expected, result)
		}
	})
}

// compareWitness performs exact ordered comparison of witness arrays.
func compareWitness(t *testing.T, blockNum uint64, expected *testutil.ExpectedWitness, actual *jsonrpc.ExecutionWitnessResult) {
	t.Helper()
	compareByteSlices(t, blockNum, "State", expected.State, actual.State)
	compareByteSlices(t, blockNum, "Codes", expected.Codes, actual.Codes)
	compareByteSlices(t, blockNum, "Headers", expected.Headers, actual.Headers)
}

// compareByteSlices compares two slices of hexutil.Bytes element-by-element.
func compareByteSlices(t *testing.T, blockNum uint64, field string, expected, actual []hexutil.Bytes) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("block %d %s: length mismatch: expected %d elements, got %d",
			blockNum, field, len(expected), len(actual))
		// Log first few elements for debugging
		limit := min(3, len(expected))
		for i := 0; i < limit; i++ {
			t.Logf("  expected[%d]: %s", i, truncHex(expected[i], 64))
		}
		limit = min(3, len(actual))
		for i := 0; i < limit; i++ {
			t.Logf("  actual[%d]:   %s", i, truncHex(actual[i], 64))
		}
		return
	}
	for i := range expected {
		if !bytes.Equal(expected[i], actual[i]) {
			t.Errorf("block %d %s[%d]: mismatch\n  expected: %s\n  actual:   %s",
				blockNum, field, i,
				truncHex(expected[i], 64),
				truncHex(actual[i], 64))
		}
	}
}

// truncHex returns a hex string of b, truncated to maxBytes with "..." suffix.
func truncHex(b []byte, maxBytes int) string {
	if len(b) <= maxBytes {
		return fmt.Sprintf("0x%s (%d bytes)", hex.EncodeToString(b), len(b))
	}
	return fmt.Sprintf("0x%s... (%d bytes)", hex.EncodeToString(b[:maxBytes]), len(b))
}
