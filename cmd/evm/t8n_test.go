package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/docker/docker/pkg/reexec"
	"github.com/ledgerwatch/erigon/turbo/cmdtest"
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
