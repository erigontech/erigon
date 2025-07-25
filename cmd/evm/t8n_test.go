// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"fmt"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/common/race"
	"github.com/erigontech/erigon/internal/reexec"
	"github.com/erigontech/erigon/turbo/cmdtest"
)

func TestMain(m *testing.M) {
	// Run the app if we've been exec'd as "ethkey-test" in runEthkey.
	reexec.Register("evm-test", func() {
		if err := app.Run(os.Args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	})
	// check if we have been reexec'd
	if reexec.Init() {
		return
	}
	os.Exit(m.Run())
}

type testT8n struct {
	*cmdtest.TestCmd
}

type t8nInput struct {
	inAlloc string
	inTxs   string
	inEnv   string
	stFork  string
}

func (args *t8nInput) get(base string) []string {
	var out []string
	if opt := args.inAlloc; opt != "" {
		out = append(out, "--input.alloc")
		out = append(out, fmt.Sprintf("%v/%v", base, opt))
	}
	if opt := args.inTxs; opt != "" {
		out = append(out, "--input.txs")
		out = append(out, fmt.Sprintf("%v/%v", base, opt))
	}
	if opt := args.inEnv; opt != "" {
		out = append(out, "--input.env")
		out = append(out, fmt.Sprintf("%v/%v", base, opt))
	}
	if opt := args.stFork; opt != "" {
		out = append(out, "--state.fork", opt)
	}
	return out
}

type t8nOutput struct {
	alloc  bool
	result bool
	body   bool
}

func (args *t8nOutput) get() (out []string) {
	if args.body {
		out = append(out, "--output.body", "stdout")
	} else {
		out = append(out, "--output.body", "") // empty means ignore
	}
	if args.result {
		out = append(out, "--output.result", "stdout")
	} else {
		out = append(out, "--output.result", "")
	}
	if args.alloc {
		out = append(out, "--output.alloc", "stdout")
	} else {
		out = append(out, "--output.alloc", "")
	}
	return out
}

func TestT8n(t *testing.T) {
	t.Skip("unstable")
	tt := new(testT8n)
	tt.TestCmd = cmdtest.NewTestCmd(t, tt)
	for i, tc := range []struct {
		base        string
		input       t8nInput
		output      t8nOutput
		expExitCode int
		expOut      string
	}{
		{ // Test exit (3) on bad config
			base: "./testdata/1",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "Frontier+1346",
			},
			output:      t8nOutput{alloc: true, result: true},
			expExitCode: 3,
		},
		{ // blockhash test
			base: "./testdata/3",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "Berlin",
			},
			output: t8nOutput{alloc: true, result: true},
			expOut: "exp.json",
		},
		{ // missing blockhash test
			base: "./testdata/4",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "Berlin",
			},
			expExitCode: 4,
		},
		{ // Uncle test
			base: "./testdata/5",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "Byzantium",
			},
			output: t8nOutput{alloc: true, result: true},
			expOut: "exp.json",
		},
		{ // Dao-transition check
			base: "./testdata/7",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "HomesteadToDaoAt5",
			},
			expOut: "exp.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // transactions with access list
			base: "./testdata/8",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "Berlin",
			},
			expOut: "exp.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // EIP-1559
			base: "./testdata/9",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "London",
			},
			expOut: "exp.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // EIP-1559
			base: "./testdata/10",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "London",
			},
			expOut: "exp.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // missing base fees
			base: "./testdata/11",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "London",
			},
			expExitCode: 3,
		},
		{ // EIP-1559 & gasCap
			base: "./testdata/12",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "London",
			},
			expOut: "exp.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // Difficulty calculation on London
			base: "./testdata/19",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "London",
			},
			expOut: "exp_london.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // Difficulty calculation on arrow glacier
			base: "./testdata/19",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "ArrowGlacier",
			},
			expOut: "exp_arrowglacier.json",
			output: t8nOutput{alloc: true, result: true},
		},
		{ // eip-4895
			base: "./testdata/26",
			input: t8nInput{
				"alloc.json", "txs.json", "env.json", "Shanghai",
			},
			expOut: "exp.json",
			output: t8nOutput{alloc: true, result: true},
		},
	} {

		args := []string{"t8n"}
		args = append(args, tc.output.get()...)
		args = append(args, tc.input.get(tc.base)...)
		var qArgs []string // quoted args for debugging purposes
		for _, arg := range args {
			if len(arg) == 0 {
				qArgs = append(qArgs, `""`)
			} else {
				qArgs = append(qArgs, arg)
			}
		}
		tt.Logf("args: %v\n", strings.Join(qArgs, " "))
		tt.Run("evm-test", args...)
		// Compare the expected output, if provided
		if tc.expOut != "" {
			want, err := os.ReadFile(fmt.Sprintf("%v/%v", tc.base, tc.expOut))
			if err != nil {
				t.Fatalf("test %d: could not read expected output: %v", i, err)
			}
			have := tt.Output()
			ok, err := cmpJson(have, want)
			switch {
			case err != nil:
				t.Fatalf("test %d, json parsing failed: %v", i, err)
			case !ok:
				t.Fatalf("test %d: output wrong, have \n%v\nwant\n%v\n", i, string(have), string(want))
			}
		}
		tt.WaitExit()
		if have, want := tt.ExitStatus(), tc.expExitCode; have != want {
			t.Fatalf("test %d: wrong exit code, have %d, want %d", i, have, want)
		}
	}
}

func TestEvmRun(t *testing.T) {
	t.Skip("todo: https://github.com/erigontech/erigon/issues/16150")
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	//goland:noinspection GoBoolExpressions
	if race.Enabled && runtime.GOOS == "darwin" {
		// We run race detector for medium tests which fails on macOS.
		// This issue has already been reported for other tests.
		// Important observation for further work on this: Only `statetest` test fails.
		t.Skip("issue #15007")
	}

	t.Parallel()
	tt := cmdtest.NewTestCmd(t, nil)
	for i, tc := range []struct {
		input          []string
		wantStdoutFile string
		wantStderrFile string
	}{
		{ // json tracing
			input:          []string{"--json", "--code", "6040", "run"},
			wantStdoutFile: "./testdata/evmrun/1.out.1.txt",
			wantStderrFile: "./testdata/evmrun/1.out.2.txt",
		},
		{ // Debug output
			input:          []string{"--debug", "--code", "6040", "run"},
			wantStdoutFile: "./testdata/evmrun/2.out.1.txt",
			wantStderrFile: "./testdata/evmrun/2.out.2.txt",
		},
		{ // bench run output
			input:          []string{"--bench", "--code", "6040", "run"},
			wantStdoutFile: "./testdata/evmrun/3.out.1.txt",
			wantStderrFile: "./testdata/evmrun/3.out.2.txt",
		},
		{ // statetest
			input:          []string{"--bench", "statetest", "./testdata/statetest.json"},
			wantStdoutFile: "./testdata/evmrun/4.out.1.txt",
			wantStderrFile: "./testdata/evmrun/4.out.2.txt",
		},
	} {
		tt.Logf("args: go run ./cmd/evm %v\n", strings.Join(tc.input, " "))
		tt.Run("evm-test", tc.input...)

		haveStdOut := tt.Output()
		tt.WaitExit()
		haveStdErr := tt.Stderr()

		checkExpectedOutput(t, haveStdOut, tc.wantStdoutFile, i)
		checkExpectedOutput(t, haveStdErr, tc.wantStderrFile, i)
	}
}

func checkExpectedOutput(t *testing.T, output []byte, expectationFilePath string, i int) {
	if len(expectationFilePath) > 0 {
		want, err := os.ReadFile(expectationFilePath)
		if err != nil {
			t.Fatalf("test %d: could not read expected output: %v", i, err)
		}

		re, err := regexp.Compile(string(want))
		if err != nil {
			t.Fatalf("test %d: could not compile regular expression: %v", i, err)
		}

		if !re.Match(output) {
			t.Fatalf("test %d, output wrong, have \n%v\nwant\n%v\n", i, string(output), string(want))
		}
	}
}

// cmpJson compares the JSON in two byte slices.
func cmpJson(a, b []byte) (bool, error) {
	var j, j2 interface{}
	if err := json.Unmarshal(a, &j); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}

	return reflect.DeepEqual(j2, j), nil
}
