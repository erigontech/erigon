// Copyright 2024 The go-ethereum Authors
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
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
)

const (
	PASS = "\033[32mPASS\033[0m"
	FAIL = "\033[31mFAIL\033[0m"
)

// testResult contains the execution status after running a block test, any
// error that might have occurred and a dump of the final state if requested.
type testResult struct {
	Name  string       `json:"name"`
	Pass  bool         `json:"pass"`
	Root  *common.Hash `json:"stateRoot,omitempty"`
	Fork  string       `json:"fork,omitempty"`
	Error string       `json:"error"`
	State *state.Dump  `json:"state,omitempty"`
	Stats *execStats   `json:"benchStats,omitempty"`
}

func (r testResult) String() string {
	var status string
	if r.Pass {
		status = fmt.Sprintf("[%s]", PASS)
	} else {
		status = fmt.Sprintf("[%s]", FAIL)
	}

	var extra string
	if !r.Pass {
		extra = fmt.Sprintf(", err=%v", r.Error)
		if r.Fork != "" {
			extra += fmt.Sprintf(", fork=%s", r.Fork)
		}
	}

	out := fmt.Sprintf("%s %s%s", status, r.Name, extra)
	if r.State != nil {
		state, _ := json.MarshalIndent(r.State, "", "  ")
		out += "\n" + string(state)
	}
	return out
}

// report prints the after-test summary.
func report(ctx *cli.Context, results []testResult) {
	if ctx.Bool(JSONOutputFlag.Name) {
		// Write directly to stdout via encoder to avoid the intermediate
		// MarshalIndent -> string -> Println allocation chain.
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(results) //nolint:errcheck
		return
	}
	pass := 0
	for _, r := range results {
		if r.Pass {
			pass++
		}
	}

	// Use buffered writer to reduce syscalls when printing many results
	w := bufio.NewWriter(os.Stdout)
	for _, r := range results {
		fmt.Fprintln(w, r)
	}
	fmt.Fprintln(w, "--")
	fmt.Fprintf(w, "%d tests passed, %d tests failed.\n", pass, len(results)-pass)
	w.Flush()
}
