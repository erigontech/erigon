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
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils/cmdtest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestExecutionPathReporterLines(t *testing.T) {
	hash := common.HexToHash("0xabcdef")
	for _, tc := range []struct {
		name       string
		sequential bool
		path       ethconfig.BlockExecutionPath
		want       string
	}{
		{
			name: "parallel with access list",
			path: ethconfig.BlockExecutionPath{Number: 12, Hash: hash, Path: "parallel", AccessList: true},
			want: `{"event":"balExecution","block":12,"hash":"0x0000000000000000000000000000000000000000000000000000000000abcdef","path":"parallel","reason":"","scheduler":"bal"}`,
		},
		{
			name: "parallel without access list",
			path: ethconfig.BlockExecutionPath{Number: 12, Hash: hash, Path: "parallel"},
			want: `{"event":"balExecution","block":12,"hash":"0x0000000000000000000000000000000000000000000000000000000000abcdef","path":"parallel","reason":"","scheduler":"optimistic"}`,
		},
		{
			name:       "single worker under --bal-sequential",
			sequential: true,
			path:       ethconfig.BlockExecutionPath{Number: 12, Hash: hash, Path: "sequential", Reason: "single-worker", AccessList: true},
			want:       `{"event":"balExecution","block":12,"hash":"0x0000000000000000000000000000000000000000000000000000000000abcdef","path":"sequential","reason":"disabled","scheduler":"bal"}`,
		},
		{
			name:       "dropped access list under --bal-sequential",
			sequential: true,
			path:       ethconfig.BlockExecutionPath{Number: 12, Hash: hash, Path: "sequential", Reason: "bad-access-list"},
			want:       `{"event":"balExecution","block":12,"hash":"0x0000000000000000000000000000000000000000000000000000000000abcdef","path":"sequential","reason":"bad-access-list","scheduler":"optimistic"}`,
		},
		{
			name: "single worker by configuration",
			path: ethconfig.BlockExecutionPath{Number: 12, Hash: hash, Path: "sequential", Reason: "single-worker", AccessList: true},
			want: `{"event":"balExecution","block":12,"hash":"0x0000000000000000000000000000000000000000000000000000000000abcdef","path":"sequential","reason":"single-worker","scheduler":"bal"}`,
		},
		{
			name:       "withheld access list under --bal-sequential",
			sequential: true,
			path:       ethconfig.BlockExecutionPath{Number: 12, Hash: hash, Path: "sequential", Reason: "withheld"},
			want:       `{"event":"balExecution","block":12,"hash":"0x0000000000000000000000000000000000000000000000000000000000abcdef","path":"sequential","reason":"withheld","scheduler":"optimistic"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			executionPathReporter(&out, tc.sequential)(tc.path)
			require.Equal(t, tc.want+"\n", out.String())
		})
	}
}

// balExecutionLines runs a runner on fixture, requires every fixture to pass
// and returns the balExecution lines it printed on stderr.
func balExecutionLines(t *testing.T, command, fixture string, flags ...string) []string {
	t.Helper()
	tt := cmdtest.NewTestCmd(t, nil)
	tt.Run("evm-test", append(append([]string{command, "--jsonout"}, flags...), fixture)...)
	stdout := tt.Output()
	tt.WaitExit()
	require.Equal(t, 0, tt.ExitStatus())

	var results []testResult
	require.NoError(t, json.Unmarshal(stdout, &results))
	require.NotEmpty(t, results)
	for _, r := range results {
		require.True(t, r.Pass, r.Name)
	}

	var lines []string
	for line := range bytes.SplitSeq(tt.Stderr(), []byte("\n")) {
		if bytes.Contains(line, []byte(`"event":"balExecution"`)) {
			lines = append(lines, string(line))
		}
	}
	return lines
}

// Both runners print a line per executed block with --bal-report, and none
// without it. Every block runs in parallel by default and sequentially under
// --bal-sequential, so the flag reaches the executor.
func TestBALReportFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	for _, runner := range []struct {
		command string
		fixture string
	}{
		{"blocktest", "../../execution/tests/testdata/missing_eest_coverage/reverted_storage_read_after_recreate_has_correct_bal_hash_github_issue_23407.json"},
		{"enginetest", "../../execution/tests/testdata/engine/bal_cross_block_ripemd160_state_leak.json"},
	} {
		for _, tc := range []struct {
			flags []string
			want  string // the path and reason on every line; no lines if empty
		}{
			{nil, ""},
			{[]string{"--bal-report"}, `"path":"parallel","reason":""`},
			{[]string{"--bal-report", "--bal-sequential"}, `"path":"sequential","reason":"disabled"`},
		} {
			t.Run(fmt.Sprintf("%s %v", runner.command, tc.flags), func(t *testing.T) {
				lines := balExecutionLines(t, runner.command, runner.fixture, tc.flags...)
				if tc.want == "" {
					require.Empty(t, lines)
					return
				}
				require.NotEmpty(t, lines)
				for _, line := range lines {
					require.Contains(t, line, tc.want)
				}
			})
		}
	}
}

// Under --bal-withhold, blocktest runs every block without its delivered
// access list, on either executor, and the fixture still passes. Only a block
// delivered with a list reports withheld; one without keeps its usual reason.
func TestBALWithholdFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	const (
		withList    = "../../execution/tests/testdata/delivered_access_list/valid.json"
		withoutList = "../../execution/tests/testdata/missing_eest_coverage/reverted_storage_read_after_recreate_has_correct_bal_hash_github_issue_23407.json"
	)
	for _, tc := range []struct {
		fixture string
		flags   []string
		want    string
	}{
		{withList, nil, `"path":"parallel","reason":"","scheduler":"bal"`},
		{withList, []string{"--bal-withhold"}, `"path":"parallel","reason":"withheld","scheduler":"optimistic"`},
		{withList, []string{"--bal-withhold", "--bal-sequential"}, `"path":"sequential","reason":"withheld","scheduler":"optimistic"`},
		{withoutList, []string{"--bal-withhold"}, `"path":"parallel","reason":"","scheduler":"optimistic"`},
		{withoutList, []string{"--bal-withhold", "--bal-sequential"}, `"path":"sequential","reason":"disabled","scheduler":"optimistic"`},
	} {
		t.Run(fmt.Sprintf("%s %v", filepath.Base(tc.fixture), tc.flags), func(t *testing.T) {
			lines := balExecutionLines(t, "blocktest", tc.fixture, append(tc.flags, "--bal-report")...)
			require.Len(t, lines, 1)
			require.Contains(t, lines[0], tc.want)
		})
	}
}
