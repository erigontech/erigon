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

package eest_zkevm_witness

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// fixturesDir resolves the extracted zkevm corpus relative to this source file
// so it is independent of the test's working directory.
func fixturesDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(thisFile)
	return filepath.Join(pkgDir, "..", "..", "..", "test-fixtures-cache", "eest_zkevm", "fixtures", "blockchain_tests")
}

func TestExecutionSpecWitness(t *testing.T) {
	// The zkevm corpus fixtures are the minimized canonical witness format, so the
	// producer must run in canonical mode regardless of the deployment default.
	t.Setenv("ERIGON_WITNESS_MODE", "canonical")

	// debug_executionWitness requires the historical-commitment schema; enable it
	// before any test DB is built and restore the prior schema once all subtests
	// (including the corpus walk) have finished.
	prevCommitment := statecfg.Schema.CommitmentDomain
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema.CommitmentDomain = prevCommitment })

	dir := fixturesDir()
	// This guard MUST run before Walk: Walk does its own t.Skip("missing test
	// files") on a missing dir, which would silently mute the whole suite.
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("zkevm fixtures not found at %s: run `make test-fixtures-zkevm`", dir)
	}

	tm := new(testutil.TestMatcher)
	// Each fixture builds a fresh MDBX DB and replays a chain; run serially to
	// keep resource use bounded.
	tm.NoParallel = true

	// zkevm@v0.4.0 charges EIP-8037 AUTH_BASE state gas on EIP-7702 clears of an
	// undelegated authority; current spec (and tests-bal@v7.2.0) refund it, so
	// these fixtures' expected gas is stale. Remove when the corpus is bumped.
	// https://github.com/erigontech/erigon/issues/21563
	for _, p := range []string{
		`delegation_clearing\.json/.*undelegated_account`,
		`delegation_clearing_and_set\.json/.*undelegated_account`,
		`delegation_clearing_tx_to\.json/.*undelegated_account`,
		`double_auth\.json/.*first_delegation_DelegationTo\.RESET`,
		`valid_tx_invalid_auth_signature\.json/.*s=SECP256K1N_OVER_2(-1)?\]`,
		`tx_to_beacon_root_contract\.json/.*tx_type_4`,
		`blobhash_gas_cost\.json/.*tx_type_4`,
		`blobhash_opcode_contexts_tx_types\.json/.*tx_type_4`,
		`bal_7702_null_address_delegation_no_code_change`,
	} {
		tm.SkipLoad(p)
	}

	// The eip8025_optional_proofs witness_validation_* fixtures are stateless-verifier
	// negative tests: each stores a deliberately mutated executionWitness, so producer
	// comparison always diverges. Need a stateless-verify consumer mode; until then skip.
	// https://github.com/erigontech/erigon/issues/21566
	for _, p := range []string{
		`witness_validation_(codes|headers|state)/[a-z0-9_]+_(missing|extra|malformed)_`,
		`witness_headers/witness_headers_extra_unused_older_ancestor`,
	} {
		tm.SkipLoad(p)
	}

	tm.Walk(t, dir, func(t *testing.T, name string, test *testutil.WitnessBlockTest) {
		runWitnessTest(t, test)
	})
}

func runWitnessTest(t *testing.T, test *testutil.WitnessBlockTest) {
	t.Helper()

	// Every zkevm block carries a blockAccessList, so the runner needs the BAL
	// (parallel) executor enabled.
	test.ExperimentalBAL = true

	m, err := test.RunWithTester(t)
	if err != nil {
		t.Fatalf("block test failed: %v", err)
	}

	rwTx, err := m.DB.BeginRw(m.Ctx)
	if err != nil {
		t.Fatalf("begin rw: %v", err)
	}
	defer rwTx.Rollback()
	if err := rawdb.WriteDBCommitmentHistoryEnabled(rwTx, true); err != nil {
		t.Fatalf("write commitment history flag: %v", err)
	}
	if err := rwTx.Commit(); err != nil {
		t.Fatalf("commit commitment history flag: %v", err)
	}

	baseApi := jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0, 0)
	debugApi := jsonrpc.NewPrivateDebugAPI(baseApi, m.DB, nil, 0, false)

	for i := 0; i < test.NumBlocks(); i++ {
		expected := test.ExpectedWitnessForBlock(i)
		if expected == nil {
			continue
		}
		// Invalid blocks carry a witness (the stateless-verifier input) but are
		// rejected during import and have no canonical number; RunWithTester
		// already asserted their rejection, so there is nothing to query here.
		if test.BlockExpectsException(i) {
			continue
		}
		blockNum, ok := test.BlockNumberForBlock(i)
		if !ok {
			t.Fatalf("block index %d has a witness but no parseable block number", i)
		}

		res, err := debugApi.ExecutionWitness(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum)), nil)
		if err != nil {
			t.Fatalf("ExecutionWitness(block %d): %v", blockNum, err)
		}
		if res == nil {
			t.Fatalf("ExecutionWitness(block %d): nil result", blockNum)
		}

		compareWitness(t, blockNum, expected, res)
	}
}

func compareWitness(t *testing.T, blockNum uint64, expected *testutil.ExpectedWitness, res *jsonrpc.ExecutionWitnessResult) {
	t.Helper()

	if !equalMultiset(expected.State, res.State) {
		t.Errorf("block %d state witness mismatch:\n  expected (%d):\n%s\n  got (%d):\n%s",
			blockNum, len(expected.State), formatBytes(expected.State), len(res.State), formatBytes(res.State))
	}
	if !equalMultiset(expected.Codes, res.Codes) {
		t.Errorf("block %d codes witness mismatch:\n  expected (%d):\n%s\n  got (%d):\n%s",
			blockNum, len(expected.Codes), formatBytes(expected.Codes), len(res.Codes), formatBytes(res.Codes))
	}

	expectedHeaders, err := expectedHeaderHashes(expected)
	if err != nil {
		t.Fatalf("block %d decode expected header: %v", blockNum, err)
	}
	gotHeaders, err := rpcHeaderHashes(res)
	if err != nil {
		t.Fatalf("block %d read rpc header hash: %v", blockNum, err)
	}
	if !equalHashMultiset(expectedHeaders, gotHeaders) {
		t.Errorf("block %d headers witness mismatch:\n  expected (%d):\n%s\n  got (%d):\n%s",
			blockNum, len(expectedHeaders), formatHashes(expectedHeaders), len(gotHeaders), formatHashes(gotHeaders))
	}
}

func equalMultiset(a, b []hexutil.Bytes) bool {
	if len(a) != len(b) {
		return false
	}
	sa := sortedBytes(a)
	sb := sortedBytes(b)
	for i := range sa {
		if !bytes.Equal(sa[i], sb[i]) {
			return false
		}
	}
	return true
}

func sortedBytes(in []hexutil.Bytes) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = in[i]
	}
	slices.SortFunc(out, bytes.Compare)
	return out
}

func expectedHeaderHashes(ew *testutil.ExpectedWitness) ([]common.Hash, error) {
	hashes := make([]common.Hash, 0, len(ew.Headers))
	for i := range ew.Headers {
		var h types.Header
		if err := rlp.DecodeBytes(ew.Headers[i], &h); err != nil {
			return nil, fmt.Errorf("header %d: %w", i, err)
		}
		hashes = append(hashes, h.Hash())
	}
	return hashes, nil
}

func rpcHeaderHashes(res *jsonrpc.ExecutionWitnessResult) ([]common.Hash, error) {
	hashes := make([]common.Hash, 0, len(res.Headers))
	for i := range res.Headers {
		var h types.Header
		if err := rlp.DecodeBytes(res.Headers[i], &h); err != nil {
			return nil, fmt.Errorf("rpc header %d: %w", i, err)
		}
		hashes = append(hashes, h.Hash())
	}
	return hashes, nil
}

func equalHashMultiset(a, b []common.Hash) bool {
	if len(a) != len(b) {
		return false
	}
	sa := slices.Clone(a)
	sb := slices.Clone(b)
	cmp := func(x, y common.Hash) int { return bytes.Compare(x[:], y[:]) }
	slices.SortFunc(sa, cmp)
	slices.SortFunc(sb, cmp)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

func formatBytes(in []hexutil.Bytes) string {
	sorted := sortedBytes(in)
	var b strings.Builder
	for _, v := range sorted {
		fmt.Fprintf(&b, "    %x\n", v)
	}
	return b.String()
}

func formatHashes(in []common.Hash) string {
	sorted := slices.Clone(in)
	slices.SortFunc(sorted, func(x, y common.Hash) int { return bytes.Compare(x[:], y[:]) })
	var b strings.Builder
	for _, v := range sorted {
		fmt.Fprintf(&b, "    %x\n", v)
	}
	return b.String()
}
