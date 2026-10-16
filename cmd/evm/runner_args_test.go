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
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils/cmdtest"
)

// Each runner runs the fixtures under every path argument, files and
// directories alike, not only the first.
func TestRunnersRunEveryPathArgument(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	const engineFixtures = "../../execution/tests/testdata/engine/"
	for _, tc := range []struct {
		command string
		paths   []string
		want    int
	}{
		// The directory holds only the file, so it runs twice.
		{"enginetest", []string{engineFixtures + "bal_cross_block_ripemd160_state_leak.json", engineFixtures}, 2},
	} {
		t.Run(tc.command, func(t *testing.T) {
			tt := cmdtest.NewTestCmd(t, nil)
			tt.Run("evm-test", append([]string{tc.command, "--jsonout"}, tc.paths...)...)
			stdout := tt.Output()
			tt.WaitExit()
			require.Equal(t, 0, tt.ExitStatus())

			var results []testResult
			require.NoError(t, json.Unmarshal(stdout, &results))
			require.Len(t, results, tc.want)
			for _, r := range results {
				require.True(t, r.Pass, r.Name)
			}
		})
	}
}

// A path argument that does not exist fails the run with exit status 2 before
// any fixture runs, instead of being skipped.
func TestRunnersRejectMissingPathArgument(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	const missing = "../../execution/tests/testdata/does-not-exist.json"
	for _, tc := range []struct {
		command string
		path    string
	}{
		{"enginetest", "../../execution/tests/testdata/engine/bal_cross_block_ripemd160_state_leak.json"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			tt := cmdtest.NewTestCmd(t, nil)
			tt.Run("evm-test", tc.command, "--jsonout", tc.path, missing)
			stdout := tt.Output()
			tt.WaitExit()
			require.Equal(t, exitIncompleteRun, tt.ExitStatus())
			require.Empty(t, stdout)
			require.Contains(t, tt.StderrText(), "does-not-exist.json")
		})
	}
}

// A fixture that cannot be decoded fails on its own with the decoder's error,
// also on stderr; the other fixtures in its file run as usual, and the run
// exits 2.
func TestRunnersRunFixturesBesideAnUnreadableOne(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	for _, tc := range []struct {
		command string
		fixture string
	}{
		{"enginetest", "../../execution/tests/testdata/engine/bal_cross_block_ripemd160_state_leak.json"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			data, err := os.ReadFile(tc.fixture)
			require.NoError(t, err)
			var fixtures map[string]map[string]any
			require.NoError(t, json.Unmarshal(data, &fixtures))
			require.Len(t, fixtures, 1)
			var fixture map[string]any
			for _, f := range fixtures {
				fixture = f
			}
			unreadable := maps.Clone(fixture)
			unreadable["genesisBlockHeader"] = map[string]any{"gasLimit": "0xzz"}
			data, err = json.Marshal(map[string]any{"a": fixture, "b": unreadable, "c": fixture})
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "batch.json")
			require.NoError(t, os.WriteFile(path, data, 0o644))

			tt := cmdtest.NewTestCmd(t, nil)
			tt.Run("evm-test", tc.command, "--jsonout", path)
			stdout := tt.Output()
			tt.WaitExit()
			require.Equal(t, exitIncompleteRun, tt.ExitStatus())
			require.Contains(t, tt.StderrText(), `fixture b: `)

			var results []testResult
			require.NoError(t, json.Unmarshal(stdout, &results))
			require.Len(t, results, 3)
			for i, name := range []string{"a", "b", "c"} {
				r := results[i]
				require.Equal(t, name, r.Name)
				require.NotNil(t, r.Rejections, name)
				require.Empty(t, *r.Rejections, name)
				if name == "b" {
					require.False(t, r.Pass)
					require.Contains(t, r.Error, `invalid hex or decimal integer "0xzz"`)
					continue
				}
				require.True(t, r.Pass, r.Error)
			}
		})
	}
}

// A fixture file that is not a JSON object is reported on stderr; the other
// files run and are reported, and the run exits 2.
func TestRunnersRunFilesBesideOneThatIsNotAnObject(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	for _, tc := range []struct {
		command string
		fixture string
	}{
		{"enginetest", "../../execution/tests/testdata/engine/bal_cross_block_ripemd160_state_leak.json"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			array := filepath.Join(t.TempDir(), "array.json")
			require.NoError(t, os.WriteFile(array, []byte("[1,2]"), 0o644))

			tt := cmdtest.NewTestCmd(t, nil)
			tt.Run("evm-test", tc.command, "--jsonout", array, tc.fixture)
			stdout := tt.Output()
			tt.WaitExit()
			require.Equal(t, exitIncompleteRun, tt.ExitStatus())
			require.Contains(t, tt.StderrText(), "array.json: json: cannot unmarshal array")

			var results []testResult
			require.NoError(t, json.Unmarshal(stdout, &results))
			require.Len(t, results, 1)
			require.True(t, results[0].Pass, results[0].Error)
		})
	}
}

// A fixture that fails exits 1 once every fixture has run.
func TestRunnersExitOneWhenAFixtureFails(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	wrongLastBlock := func(fixture map[string]any) {
		fixture["lastblockhash"] = "0x" + strings.Repeat("11", 32)
	}
	for _, tc := range []struct {
		command string
		fixture string
	}{
		{"enginetest", "../../execution/tests/testdata/engine/bal_cross_block_ripemd160_state_leak.json"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			tt := cmdtest.NewTestCmd(t, nil)
			tt.Run("evm-test", tc.command, "--jsonout", writeEditedFixture(t, tc.fixture, wrongLastBlock))
			stdout := tt.Output()
			tt.WaitExit()
			require.Equal(t, exitFailedRun, tt.ExitStatus(), tt.StderrText())

			var results []testResult
			require.NoError(t, json.Unmarshal(stdout, &results))
			require.Len(t, results, 1)
			require.False(t, results[0].Pass)
		})
	}
}
