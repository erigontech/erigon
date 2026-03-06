// Copyright 2017 The go-ethereum Authors
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

package testutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/erigontech/erigon/execution/chain"
)

// TestMatcher controls skipping and chain config assignment to tests.
type TestMatcher struct {
	configpat    []testConfig
	failpat      []testFailure
	skiploadpat  []*regexp.Regexp
	slowpat      []*regexp.Regexp
	whitelistpat *regexp.Regexp
	NoParallel   bool
}

type testConfig struct {
	p      *regexp.Regexp
	config *chain.Config
}

type testFailure struct {
	p      *regexp.Regexp
	reason string
}

// Slow marks tests matching pattern as slow; skipped in -short mode and on Windows.
func (tm *TestMatcher) Slow(pattern string) {
	if runtime.GOOS == "windows" {
		tm.SkipLoad(pattern)
		return
	}
	tm.slowpat = append(tm.slowpat, regexp.MustCompile(pattern))
}

// SkipLoad skips JSON loading of tests matching the pattern.
func (tm *TestMatcher) SkipLoad(pattern string) {
	tm.skiploadpat = append(tm.skiploadpat, regexp.MustCompile(pattern))
}

// Fails adds an expected failure for tests matching the pattern.
func (tm *TestMatcher) Fails(pattern string, reason string) {
	if reason == "" {
		panic("empty fail reason")
	}
	tm.failpat = append(tm.failpat, testFailure{regexp.MustCompile(pattern), reason})
}

// Whitelist restricts test execution to tests matching pattern.
func (tm *TestMatcher) Whitelist(pattern string) {
	tm.whitelistpat = regexp.MustCompile(pattern)
}

// Config defines chain config for tests matching the pattern.
func (tm *TestMatcher) Config(pattern string, cfg *chain.Config) {
	tm.configpat = append(tm.configpat, testConfig{regexp.MustCompile(pattern), cfg})
}

// FindSkip matches name against test skip patterns.
func (tm *TestMatcher) FindSkip(name string) (reason string, skipload bool) {
	isWin32 := runtime.GOARCH == "386" && runtime.GOOS == "windows"
	for _, re := range tm.slowpat {
		if re.MatchString(name) {
			if testing.Short() {
				return "skipped in -short mode", false
			}
			if isWin32 {
				return "skipped on 32bit windows", false
			}
		}
	}
	for _, re := range tm.skiploadpat {
		if re.MatchString(name) {
			return "skipped by skipLoad", true
		}
	}
	return "", false
}

// CheckFailure checks whether a failure is expected.
func (tm *TestMatcher) CheckFailure(t *testing.T, err error) error {
	return tm.CheckFailureWithName(t, t.Name(), err)
}

// CheckFailureWithName checks whether a failure is expected for the given name.
func (tm *TestMatcher) CheckFailureWithName(t *testing.T, name string, err error) error {
	failReason := ""
	for _, m := range tm.failpat {
		if m.p.MatchString(t.Name()) {
			failReason = m.reason
			break
		}
	}
	if failReason != "" {
		t.Logf("expected failure: %s", failReason)
		if err != nil {
			t.Logf("error: %v", err)
			return nil
		}
		return errors.New("test succeeded unexpectedly")
	}
	return err
}

// Walk invokes its runTest argument for all subtests in the given directory.
//
// runTest should be a function of type func(t *testing.T, name string, x <TestType>),
// where TestType is the type of the test contained in test files.
func (tm *TestMatcher) Walk(t *testing.T, dir string, runTest any) {
	// Walk the directory.
	dirinfo, err := os.Stat(dir)
	if os.IsNotExist(err) || !dirinfo.IsDir() {
		fmt.Fprintf(os.Stderr, "can't find test files in %s, did you clone the tests submodule?\n", dir)
		t.Skip("missing test files")
	}
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //skip magically disappeared files
				return nil
			}
			return err
		}
		name := filepath.ToSlash(strings.TrimPrefix(path, dir+string(filepath.Separator)))
		if d.IsDir() {
			if _, skipload := tm.FindSkip(name + "/"); skipload {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) == ".json" {
			t.Run(name, func(t *testing.T) { tm.runTestFile(t, path, name, runTest) })
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (tm *TestMatcher) runTestFile(t *testing.T, path, name string, runTest any) {
	if !tm.NoParallel {
		t.Parallel()
	}
	if r, _ := tm.FindSkip(name); r != "" {
		t.Skip(r)
	}
	if tm.whitelistpat != nil {
		if !tm.whitelistpat.MatchString(t.Name()) {
			t.Skip("Skipped by whitelist")
		}
	}

	// Load the file as map[string]<testType>.
	m := makeMapFromTestFunc(runTest)
	if err := readJSONFile(path, m.Addr().Interface()); err != nil {
		t.Fatal(err)
	}

	// Run all tests from the map. Don't wrap in a subtest if there is only one test in the file.
	keys := sortedMapKeys(m)
	if len(keys) == 1 {
		runTestFunc(runTest, t, name, m, keys[0])
	} else {
		for _, key := range keys {
			name := name + "/" + key
			t.Run(key, func(t *testing.T) {
				if r, _ := tm.FindSkip(name); r != "" {
					t.Skip(r)
				}
				runTestFunc(runTest, t, name, m, key)
			})
		}
	}
}

func readJSONFile(fn string, value any) error {
	data, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("error reading JSON file: %w", err)
	}

	if err = json.Unmarshal(data, &value); err != nil {
		if syntaxerr, ok := err.(*json.SyntaxError); ok {
			line := findLine(data, syntaxerr.Offset)
			return fmt.Errorf("JSON syntax error at line %v: %w", line, err)
		}
		return err
	}
	return nil
}

// findLine returns the line number for the given offset into data.
func findLine(data []byte, offset int64) (line int) {
	line = 1
	for i, r := range string(data) {
		if int64(i) >= offset {
			return
		}
		if r == '\n' {
			line++
		}
	}
	return
}

func makeMapFromTestFunc(f any) reflect.Value {
	stringT := reflect.TypeFor[string]()
	testingT := reflect.TypeFor[*testing.T]()
	ftyp := reflect.TypeOf(f)
	if ftyp.Kind() != reflect.Func || ftyp.NumIn() != 3 || ftyp.NumOut() != 0 || ftyp.In(0) != testingT || ftyp.In(1) != stringT {
		panic(fmt.Sprintf("bad test function type: want func(*testing.T, string, <TestType>), have %s", ftyp))
	}
	testType := ftyp.In(2)
	mp := reflect.New(reflect.MapOf(stringT, testType))
	return mp.Elem()
}

func sortedMapKeys(m reflect.Value) []string {
	keys := make([]string, m.Len())
	for i, k := range m.MapKeys() {
		keys[i] = k.String()
	}
	slices.Sort(keys)
	return keys
}

func runTestFunc(runTest any, t *testing.T, name string, m reflect.Value, key string) {
	reflect.ValueOf(runTest).Call([]reflect.Value{
		reflect.ValueOf(t),
		reflect.ValueOf(name),
		m.MapIndex(reflect.ValueOf(key)),
	})
}
