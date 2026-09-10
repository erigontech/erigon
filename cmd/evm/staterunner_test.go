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
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestNewStateTestSharedDomainsUsesSelectedCommitment(t *testing.T) {
	originalParallel := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalParallelCommitment = originalParallel
	})

	for _, tc := range []struct {
		name     string
		parallel bool
		variant  commitment.TrieVariant
	}{
		{name: "serial", variant: commitment.VariantHexPatriciaTrie},
		{name: "parallel", parallel: true, variant: commitment.VariantParallelHexPatricia},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statecfg.ExperimentalParallelCommitment = tc.parallel

			db, tx := temporaltest.NewTestTx(t)
			sd, err := newStateTestSharedDomains(db, tx)
			require.NoError(t, err)
			t.Cleanup(sd.Close)

			require.Equal(t, tc.variant, sd.GetCommitmentCtx().Trie().Variant())
		})
	}
}

// Batch mode reuses one scratch DB for every file on stdin, so a subtest's
// writes must not survive into the next file: interleaving fixtures has to
// give each of them the same result as running it alone.
func TestStateTestBatchModeIsolatesFiles(t *testing.T) {
	cornersDir := filepath.Join("..", "..", "execution", "tests", "test-corners", "state")
	files := []string{
		filepath.Join("testdata", "statetest.json"),
		filepath.Join(cornersDir, "CallNonExistingAccount.json"),
		filepath.Join(cornersDir, "eip2681-max-sender-nonce.json"),
		filepath.Join(cornersDir, "SingletonStorageCell_UpdateKindPropagate_AllTheWayUpToRoot.json"),
	}

	alone := map[string][]testResult{}
	for _, f := range files {
		alone[f] = runStateTestBatch(t, []string{f})
	}

	interleaved := append(append([]string{}, files...), files...)
	got := runStateTestBatch(t, interleaved)

	var want []testResult
	for _, f := range interleaved {
		want = append(want, alone[f]...)
	}
	require.Equal(t, want, got)
}

// runStateTestBatch feeds files to `statetest` on stdin and decodes the
// per-file JSON reports it writes to stdout.
func runStateTestBatch(t *testing.T, files []string) []testResult {
	t.Helper()

	stdin, err := os.CreateTemp(t.TempDir(), "stdin")
	require.NoError(t, err)
	defer stdin.Close()
	_, err = stdin.WriteString(strings.Join(files, "\n") + "\n")
	require.NoError(t, err)
	_, err = stdin.Seek(0, io.SeekStart)
	require.NoError(t, err)

	stdout, err := os.CreateTemp(t.TempDir(), "stdout")
	require.NoError(t, err)
	defer stdout.Close()

	origIn, origOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdin, stdout
	defer func() { os.Stdin, os.Stdout = origIn, origOut }()

	cmd := &cli.Command{Commands: []*cli.Command{&stateTestCommand}}
	require.NoError(t, cmd.Run(context.Background(), []string{"evm", "statetest", "--jsonout"}))

	_, err = stdout.Seek(0, io.SeekStart)
	require.NoError(t, err)
	var results []testResult
	batches := 0
	dec := json.NewDecoder(stdout)
	for {
		var batch []testResult
		if err := dec.Decode(&batch); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		batches++
		results = append(results, batch...)
	}
	require.Equal(t, len(files), batches, "one JSON report per input file")
	require.NotEmpty(t, results)
	return results
}
