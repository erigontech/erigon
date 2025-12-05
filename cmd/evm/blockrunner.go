// Copyright 2023 The go-ethereum Authors
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
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

var blockTestCommand = cli.Command{
	Action:    blockTestCmd,
	Name:      "blocktest",
	Usage:     "Executes the given blockchain tests",
	ArgsUsage: "<path>",
	Flags: []cli.Flag{
		&DumpFlag,
		&VerbosityFlag,
	},
}

func blockTestCmd(ctx *cli.Context) error {
	path := ctx.Args().First()
	if len(path) == 0 {
		return errors.New("path argument required")
	}

	// Set up logging
	if ctx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	var (
		collected = collectFiles(path)
		results   []testResult
	)

	for _, fname := range collected {
		r, err := runBlockTest(ctx, fname)
		if err != nil {
			return err
		}
		results = append(results, r...)
	}

	report(ctx, results)
	return nil
}

// collectFiles walks the given path and returns all JSON files.
// If path is a file, it returns that file directly.
func collectFiles(path string) []string {
	var out []string
	info, err := os.Stat(path)
	if err != nil {
		return out
	}

	if !info.IsDir() {
		return []string{path}
	}

	err = filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".json" {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error walking path %s: %v\n", path, err)
	}

	return out
}

func runBlockTest(ctx *cli.Context, fname string) ([]testResult, error) {
	src, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}

	var tests map[string]*testutil.BlockTest
	if err = json.Unmarshal(src, &tests); err != nil {
		return nil, err
	}

	re, err := regexp.Compile(".*") // Run all tests by default
	if err != nil {
		return nil, fmt.Errorf("invalid regex: %v", err)
	}

	// Pull out keys to sort and ensure tests are run in order
	keys := slices.Sorted(maps.Keys(tests))

	// Run all the tests
	results := make([]testResult, 0, len(keys))
	for _, name := range keys {
		if !re.MatchString(name) {
			continue
		}

		result := &testResult{Name: name, Pass: true}
		if err := tests[name].RunCLI(); err != nil {
			result.Pass = false
			result.Error = err.Error()
		}

		results = append(results, *result)
	}

	return results, nil
}
