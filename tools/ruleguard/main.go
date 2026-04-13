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

// ruleguard is a small go vet -vettool wrapper around the ruleguard analyzer.
//
// Running ruleguard via gocritic inside golangci-lint creates a fresh
// ruleguard engine per analysis Pass (per package), which is ~50s of fixed
// overhead on this repo. Running it as a go vet tool lets Go's vet cache
// key analyzer results on package content plus tool binary hash, so only
// changed packages re-analyze. See tools/ruleguard/rules.go for rules.
//
// The rules file is embedded at build time so that any change to it changes
// the tool binary hash, which in turn invalidates the go vet cache.
//
// This wrapper also honors golangci-lint-style //nolint:gocritic and
// //nolint:ruleguard directives, which the upstream ruleguard analyzer
// ignores. A nolint comment suppresses diagnostics reported on the same
// line as the comment or on the line immediately after it.
package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"

	"github.com/quasilyte/go-ruleguard/analyzer"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/unitchecker"
)

//go:embed rules.go
var rulesSrc []byte

// wrappedAnalyzer delegates to ruleguard but filters diagnostics on
// //nolint lines before they reach the driver.
var wrappedAnalyzer = &analysis.Analyzer{
	Name:  analyzer.Analyzer.Name,
	Doc:   analyzer.Analyzer.Doc,
	Flags: analyzer.Analyzer.Flags,
	Run:   runWithNolint,
}

func runWithNolint(pass *analysis.Pass) (interface{}, error) {
	if isExcludedPackage(pass.Pkg.Path()) {
		return nil, nil
	}
	suppressed := collectNolintLines(pass)
	origReport := pass.Report
	pass.Report = func(d analysis.Diagnostic) {
		pos := pass.Fset.Position(d.Pos)
		if isExcludedFile(pos.Filename) {
			return
		}
		if suppressed[pos.Line] {
			return
		}
		origReport(d)
	}
	return analyzer.Analyzer.Run(pass)
}

// modulePrefix is the Go module path for this repo. go vet -vettool invokes
// the analyzer on every transitive dependency (including stdlib), so we
// short-circuit anything that isn't ours before the ruleguard engine is
// built — on a full repo run this skips ~300 stdlib packages and saves
// ~30s of engine-init cost.
const modulePrefix = "github.com/erigontech/erigon"

// Path exclusions mirror the gocritic exclusions in .golangci.yml so that
// existing per-file / per-package allowances keep working after the
// ruleguard-via-gocritic path is removed.
func isExcludedPackage(pkgPath string) bool {
	if !strings.HasPrefix(pkgPath, modulePrefix) {
		return true
	}
	return strings.Contains(pkgPath, "/cmd/devp2p") ||
		strings.HasSuffix(pkgPath, "/p2p/dnsdisc") ||
		strings.HasSuffix(pkgPath, "/tools/ruleguard")
}

func isExcludedFile(filename string) bool {
	base := filepath.Base(filename)
	if base == "hack.go" || (base == "sample.go" && strings.Contains(filename, "/metrics/")) {
		return true
	}
	return false
}

// collectNolintLines returns the set of source lines (1-indexed) whose
// diagnostics should be suppressed. A comment of the form
//
//	//nolint                       (bare)
//	//nolint:foo,gocritic,bar      (comma list)
//	//nolint:gocritic              (single)
//
// suppresses diagnostics on the comment's own line (inline trailing
// comment) and on the line immediately after (stand-alone line comment).
// We only honor bare nolint, nolint:gocritic, and nolint:ruleguard —
// since this tool replaces gocritic's ruleguard integration, existing
// //nolint:gocritic comments keep working.
func collectNolintLines(pass *analysis.Pass) map[int]bool {
	out := map[int]bool{}
	for _, f := range pass.Files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if !isRuleguardNolint(c.Text) {
					continue
				}
				line := pass.Fset.Position(c.Slash).Line
				out[line] = true
				out[line+1] = true
			}
		}
	}
	return out
}

func isRuleguardNolint(text string) bool {
	// Strip // or /* */ wrapper.
	text = strings.TrimPrefix(text, "//")
	text = strings.TrimPrefix(text, "/*")
	text = strings.TrimSuffix(text, "*/")
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "nolint") {
		return false
	}
	rest := strings.TrimPrefix(text, "nolint")
	if rest == "" || strings.HasPrefix(rest, " ") {
		return true // bare "//nolint" or "//nolint <explanation>"
	}
	if !strings.HasPrefix(rest, ":") {
		return false
	}
	// nolint:a,b,c — split on commas, stop at whitespace (golangci-lint
	// allows an optional "// explanation" suffix).
	list := strings.TrimPrefix(rest, ":")
	if i := strings.IndexAny(list, " \t"); i >= 0 {
		list = list[:i]
	}
	for _, name := range strings.Split(list, ",") {
		switch strings.TrimSpace(name) {
		case "gocritic", "ruleguard", "all":
			return true
		}
	}
	return false
}

func main() {
	// Write rules.go to a content-addressed path in TMPDIR. `go vet` spawns
	// our binary once per package in parallel; if they all truncated and
	// rewrote the same file, concurrent readers would see empty content.
	// Hashing the content into the filename means the file only needs to
	// be written once per rules.go change, and we write atomically via
	// tempfile+rename so first-writer races are safe.
	sum := sha256.Sum256(rulesSrc)
	rulesPath := filepath.Join(os.TempDir(), "erigon-ruleguard-rules-"+hex.EncodeToString(sum[:8])+".go")
	if _, err := os.Stat(rulesPath); err != nil {
		tmp, err := os.CreateTemp(os.TempDir(), "erigon-ruleguard-rules-*.go")
		if err != nil {
			panic(err)
		}
		if _, err := tmp.Write(rulesSrc); err != nil {
			panic(err)
		}
		if err := tmp.Close(); err != nil {
			panic(err)
		}
		if err := os.Rename(tmp.Name(), rulesPath); err != nil {
			// Lost the race — another process put the final file in place.
			// That's fine; drop our temp and continue.
			_ = os.Remove(tmp.Name())
		}
	}
	if err := analyzer.Analyzer.Flags.Set("rules", rulesPath); err != nil {
		panic(err)
	}
	unitchecker.Main(wrappedAnalyzer)
}
