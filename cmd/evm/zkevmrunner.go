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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

var zkevmTestCommand = cli.Command{
	Action:    zkevmTestCmd,
	Name:      "zkevmtest",
	Usage:     "Executes the EEST zkevm execution-witness conformance fixtures (blockchain_tests with per-block executionWitness)",
	ArgsUsage: "<path>",
	Description: "Each fixture imports its chain into a short-lived in-memory Erigon node\n" +
		"with the historical-commitment schema enabled, then queries\n" +
		"debug_executionWitness in canonical mode for every block and multiset-\n" +
		"compares the returned state/codes/headers against the fixture's\n" +
		"executionWitness. Blocks marked expectException are skipped (rejected on\n" +
		"import, no canonical number). The process always exits 0 — pass/fail is\n" +
		"decided by the caller from the --jsonout report.",
	Flags: []cli.Flag{
		&JSONOutputFlag,
		&RunFlag,
		&VerbosityFlag,
		&WorkersFlag,
	},
}

func zkevmTestCmd(_ context.Context, ctx *cli.Command) error {
	path := ctx.Args().First()
	if path == "" {
		return errors.New("path argument required")
	}

	if ctx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	workers := ctx.Uint64(WorkersFlag.Name)
	if workers == 0 {
		return fmt.Errorf("--%s must be >= 1", WorkersFlag.Name)
	}
	re, err := regexp.Compile(ctx.String(RunFlag.Name))
	if err != nil {
		return fmt.Errorf("invalid --run regex: %w", err)
	}

	// debug_executionWitness requires the historical-commitment schema; enable it
	// process-wide before any test DB is built. Set once here so the parallel
	// workers below only ever read it.
	statecfg.EnableHistoricalCommitment()

	files, err := collectFiles(path)
	if err != nil {
		return err
	}
	witnesses, err := loadZkevmWitnesses(files)
	if err != nil {
		return err
	}
	results, err := runZkevmTestsParallel(path, files, re, witnesses, workers)
	if err != nil {
		return err
	}
	report(ctx, results)
	return nil
}

// newZkevmFailMatcher mirrors the expected-failure set the go-test witness suite
// declared: fixtures that diverge for known, tracked reasons are reported as
// passes, and as failures only if they unexpectedly stop diverging.
func newZkevmFailMatcher() *testutil.TestMatcher {
	tm := new(testutil.TestMatcher)

	// These witness_validation_* fixtures deliberately store a mutated executionWitness for
	// stateless verifiers; a stateful EL only produces the canonical witness, so producer
	// comparison can never match them — out of scope, not pending work. See #21566.
	const verifierNegative = "verifier-negative fixture: stateful EL can't reproduce a mutated witness, see #21566"
	for _, p := range []string{
		`witness_validation_(codes|headers|state)/[a-z0-9_]+_(missing|extra|malformed)_`,
		`witness_headers/witness_headers_extra_unused_older_ancestor`,
	} {
		tm.Fails(p, verifierNegative)
	}

	return tm
}

type zkevmWitnessKey struct {
	network string
	hash    common.Hash
}

// loadZkevmWitnesses returns the reference witness of each block. A nil
// reference means every fixture's own witness for that block equals it, so
// only the few references that differ somewhere stay in memory.
func loadZkevmWitnesses(files []string) (map[zkevmWitnessKey]*testutil.ExpectedWitness, error) {
	witnesses := make(map[zkevmWitnessKey]*testutil.ExpectedWitness)
	ambiguous := make(map[zkevmWitnessKey]bool)
	digests := make(map[zkevmWitnessKey]common.Hash)
	differs := make(map[zkevmWitnessKey]bool)
	tm := newZkevmFailMatcher()
	for _, fname := range files {
		err := decodeFixtures(fname, func(name string, raw json.RawMessage) error {
			excluded := tm.CheckFailureForName(filepath.ToSlash(fname)+"/"+name, nil) != nil
			var test struct {
				Network string
				Blocks  []struct {
					BlockHeader     struct{ Hash common.Hash }
					ExpectException string
					Output          hexutil.Bytes             `json:"statelessOutputBytes"`
					Witness         *testutil.ExpectedWitness `json:"executionWitness"`
				}
			}
			if err := json.Unmarshal(raw, &test); err != nil {
				return err
			}
			for _, block := range test.Blocks {
				if block.Witness == nil || block.ExpectException != "" {
					continue
				}
				key := zkevmWitnessKey{test.Network, block.BlockHeader.Hash}
				digest := witnessDigest(block.Witness)
				if previous, ok := digests[key]; !ok {
					digests[key] = digest
				} else if previous != digest {
					differs[key] = true
				}
				if excluded {
					continue
				}
				if len(block.Output) != 43 || block.Output[32] > 1 {
					return errors.New("invalid stateless validation result")
				}
				if block.Output[32] == 0 {
					continue
				}
				candidate, previous := block.Witness, witnesses[key]
				count := len(candidate.State) + len(candidate.Codes) + len(candidate.Headers)
				previousCount := 0
				if previous != nil {
					previousCount = len(previous.State) + len(previous.Codes) + len(previous.Headers)
				}
				if previous == nil || count < previousCount {
					witnesses[key] = candidate
					delete(ambiguous, key)
				} else if count == previousCount && (!equalMultiset(candidate.State, previous.State) || !equalMultiset(candidate.Codes, previous.Codes) || !equalMultiset(candidate.Headers, previous.Headers)) {
					ambiguous[key] = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", fname, err)
		}
	}
	for key := range ambiguous {
		return nil, fmt.Errorf("ambiguous canonical witness for %s block %s", key.network, key.hash)
	}
	for key := range witnesses {
		if !differs[key] {
			witnesses[key] = nil
		}
	}
	return witnesses, nil
}

// witnessDigest identifies a witness regardless of the order of its elements.
func witnessDigest(w *testutil.ExpectedWitness) common.Hash {
	h := sha256.New()
	for _, section := range [][]hexutil.Bytes{w.State, w.Codes, w.Headers} {
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(section))))
		for _, b := range sortedBytes(section) {
			h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
			h.Write(b)
		}
	}
	return common.Hash(h.Sum(nil))
}

func runZkevmTestsParallel(root string, files []string, re *regexp.Regexp, witnesses map[zkevmWitnessKey]*testutil.ExpectedWitness, workers uint64) ([]testResult, error) {
	if workers == 1 {
		results := make([]testResult, 0, len(files)*4)
		for _, fname := range files {
			r, err := runZkevmTestFile(root, fname, re, witnesses)
			if err != nil {
				return nil, err
			}
			results = append(results, r...)
		}
		return results, nil
	}

	var (
		wg     sync.WaitGroup
		fileCh = make(chan struct {
			index int
			fname string
		}, len(files))
		resultCh = make(chan fileResult, len(files))
	)
	for i, fname := range files {
		fileCh <- struct {
			index int
			fname string
		}{i, fname}
	}
	close(fileCh)

	for range workers {
		wg.Go(func() {
			for item := range fileCh {
				r, err := runZkevmTestFile(root, item.fname, re, witnesses)
				resultCh <- fileResult{index: item.index, results: r, err: err}
			}
		})
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	ordered := make([]fileResult, len(files))
	for fr := range resultCh {
		if fr.err != nil {
			return nil, fr.err
		}
		ordered[fr.index] = fr
	}
	total := 0
	for _, fr := range ordered {
		total += len(fr.results)
	}
	results := make([]testResult, 0, total)
	for _, fr := range ordered {
		results = append(results, fr.results...)
	}
	return results, nil
}

func runZkevmTestFile(root, fname string, re *regexp.Regexp, witnesses map[zkevmWitnessKey]*testutil.ExpectedWitness) ([]testResult, error) {
	relPath := relName(root, fname)
	var results []testResult
	err := decodeFixtures(fname, func(key string, raw json.RawMessage) error {
		name := relPath + "/" + key
		if !re.MatchString(name) {
			return nil
		}
		test := new(testutil.WitnessBlockTest)
		if err := json.Unmarshal(raw, test); err != nil {
			return err
		}
		result := testResult{Name: name, Pass: true}
		if err := runWitnessTest(test, witnesses, name); err != nil {
			result.Pass = false
			result.Error = err.Error()
		}
		results = append(results, result)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", fname, err)
	}
	slices.SortFunc(results, func(a, b testResult) int { return strings.Compare(a.Name, b.Name) })
	return results, nil
}

// decodeFixtures streams the fixtures of fname to fn one at a time: some
// fixture files are hundreds of MB, and decoding a whole file at once would
// keep every fixture of every worker's current file in memory.
func decodeFixtures(fname string, fn func(name string, raw json.RawMessage) error) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return errors.New("not a JSON object of fixtures")
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		if err := fn(tok.(string), raw); err != nil {
			return fmt.Errorf("fixture %s: %w", tok, err)
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data after the fixtures object")
	}
	return nil
}

// relName returns fname relative to root, slash-normalised, so the resulting
// test name matches the subtest path the go-test suite used (and thus the
// expected-failure patterns still apply).
func relName(root, fname string) string {
	rel, err := filepath.Rel(root, fname)
	if err != nil {
		return filepath.ToSlash(fname)
	}
	return filepath.ToSlash(rel)
}

// runWitnessTest imports the fixture chain and compares debug_executionWitness
// (canonical mode) against the per-block executionWitness, returning a non-nil
// error on block-import failure or witness divergence.
func runWitnessTest(test *testutil.WitnessBlockTest, witnesses map[zkevmWitnessKey]*testutil.ExpectedWitness, name string) error {
	// Every zkevm block carries a blockAccessList, so the BAL (parallel) executor
	// must be enabled.
	test.ExperimentalBAL = true

	m, err := test.RunWithTesterCLI()
	if m != nil {
		defer m.Close()
	}
	if err != nil {
		return fmt.Errorf("block test failed: %w", err)
	}

	rwTx, err := m.DB.BeginRw(m.Ctx)
	if err != nil {
		return fmt.Errorf("begin rw: %w", err)
	}
	defer rwTx.Rollback()
	if err := rawdb.WriteDBCommitmentHistoryEnabled(rwTx, true); err != nil {
		return fmt.Errorf("write commitment history flag: %w", err)
	}
	if err := rwTx.Commit(); err != nil {
		return fmt.Errorf("commit commitment history flag: %w", err)
	}

	baseApi := jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	debugApi := jsonrpc.NewPrivateDebugAPI(baseApi, m.DB, nil, rpccfg.DefaultDebugApiConfig())
	canonicalMode := "canonical"
	var fallbackErr error
	hasFallback := false

	for i := 0; i < test.NumBlocks(); i++ {
		expected := test.ExpectedWitnessForBlock(i)
		if expected == nil {
			continue
		}
		// Invalid blocks carry a witness (the stateless-verifier input) but are
		// rejected during import and have no canonical number.
		if test.BlockExpectsException(i) {
			continue
		}
		blockNum, ok := test.BlockNumberForBlock(i)
		if !ok {
			return fmt.Errorf("block index %d has a witness but no parseable block number", i)
		}
		reference, referenced := witnesses[zkevmWitnessKey{test.Network(), test.BlockHashForBlock(i)}]
		if reference != nil {
			expected = reference
		}

		res, err := debugApi.ExecutionWitness(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum)), &canonicalMode)
		if err != nil {
			return fmt.Errorf("ExecutionWitness(block %d): %w", blockNum, err)
		}
		if res == nil {
			return fmt.Errorf("ExecutionWitness(block %d): nil result", blockNum)
		}

		err = compareWitness(blockNum, expected, res)
		if !referenced {
			hasFallback = true
			if fallbackErr == nil {
				fallbackErr = err
			}
		} else if err != nil {
			return err
		}
	}
	if hasFallback {
		return newZkevmFailMatcher().CheckFailureForName(name, fallbackErr)
	}
	return nil
}

func compareWitness(blockNum uint64, expected *testutil.ExpectedWitness, res *jsonrpc.ExecutionWitnessResult) error {
	if !equalMultiset(expected.State, res.State) {
		return fmt.Errorf("block %d state witness mismatch:\n  expected (%d):\n%s\n  got (%d):\n%s",
			blockNum, len(expected.State), formatBytes(expected.State), len(res.State), formatBytes(res.State))
	}
	if !equalMultiset(expected.Codes, res.Codes) {
		return fmt.Errorf("block %d codes witness mismatch:\n  expected (%d):\n%s\n  got (%d):\n%s",
			blockNum, len(expected.Codes), formatBytes(expected.Codes), len(res.Codes), formatBytes(res.Codes))
	}

	expectedHeaders, err := expectedHeaderHashes(expected)
	if err != nil {
		return fmt.Errorf("block %d decode expected header: %w", blockNum, err)
	}
	gotHeaders, err := rpcHeaderHashes(res)
	if err != nil {
		return fmt.Errorf("block %d read rpc header hash: %w", blockNum, err)
	}
	if !equalHashMultiset(expectedHeaders, gotHeaders) {
		return fmt.Errorf("block %d headers witness mismatch:\n  expected (%d):\n%s\n  got (%d):\n%s",
			blockNum, len(expectedHeaders), formatHashes(expectedHeaders), len(gotHeaders), formatHashes(gotHeaders))
	}
	return nil
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
