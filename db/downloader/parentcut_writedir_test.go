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

package downloader

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

func minimalForkChainConfig() *chain.Config {
	return &chain.Config{
		ChainName: "mainnet-fork-20000000",
		Parent:    "mainnet",
		CutBlock:  20_000_000,
	}
}

func TestWriteForkDatadir_CopiesFilesAndWritesChainConfig(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(t.TempDir(), "fork-snapshots")
	configPath := filepath.Join(t.TempDir(), "chain.json")

	// Populate parent with two pre-cut files.
	require.NoError(t, os.WriteFile(filepath.Join(parent, "a.seg"), []byte("AAAA"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(parent, "domain"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "domain", "b.kv"), []byte("BB"), 0o644))

	plan := &CopyPlan{
		Copy: []CopyPlanEntry{
			{RelPath: "a.seg", Classification: CopyPreCut},
			{RelPath: "domain/b.kv", Classification: CopyPreCut},
		},
	}

	files, bytes, err := WriteForkDatadir(WriteForkDatadirOpts{
		ParentSnapDir:       parent,
		ForkSnapDir:         target,
		ForkChainConfig:     minimalForkChainConfig(),
		ForkChainConfigPath: configPath,
		Plan:                plan,
	})
	require.NoError(t, err)
	require.Equal(t, 2, files)
	require.Equal(t, int64(6), bytes, "AAAA + BB = 6 bytes")

	// Files landed at expected paths with correct contents.
	gotA, err := os.ReadFile(filepath.Join(target, "a.seg"))
	require.NoError(t, err)
	require.Equal(t, []byte("AAAA"), gotA)
	gotB, err := os.ReadFile(filepath.Join(target, "domain", "b.kv"))
	require.NoError(t, err)
	require.Equal(t, []byte("BB"), gotB)

	// chain.json written with derived chain.Config fields.
	cfgBytes, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var got chain.Config
	require.NoError(t, json.Unmarshal(cfgBytes, &got))
	require.Equal(t, "mainnet-fork-20000000", got.ChainName)
	require.Equal(t, "mainnet", got.Parent)
	require.Equal(t, uint64(20_000_000), got.CutBlock)
}

func TestWriteForkDatadir_DefaultsChainConfigPathBesideSnapDir(t *testing.T) {
	parent := t.TempDir()
	datadir := t.TempDir()
	target := filepath.Join(datadir, "snapshots")

	require.NoError(t, os.WriteFile(filepath.Join(parent, "a.seg"), []byte("x"), 0o644))
	plan := &CopyPlan{Copy: []CopyPlanEntry{{RelPath: "a.seg"}}}

	_, _, err := WriteForkDatadir(WriteForkDatadirOpts{
		ParentSnapDir:   parent,
		ForkSnapDir:     target,
		ForkChainConfig: minimalForkChainConfig(),
		Plan:            plan,
	})
	require.NoError(t, err)
	// Default path = datadir/chain.json (sibling of snapshots/).
	_, err = os.Stat(filepath.Join(datadir, "chain.json"))
	require.NoError(t, err, "chain.json should default to sibling of snap dir")
}

func TestWriteForkDatadir_RefusesNonEmptyTarget(t *testing.T) {
	parent := t.TempDir()
	target := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "chain.json")
	// Pre-populate target — fork-from must refuse to clobber.
	require.NoError(t, os.WriteFile(filepath.Join(target, "existing.bin"), []byte{}, 0o644))

	plan := &CopyPlan{}
	_, _, err := WriteForkDatadir(WriteForkDatadirOpts{
		ParentSnapDir:       parent,
		ForkSnapDir:         target,
		ForkChainConfig:     minimalForkChainConfig(),
		ForkChainConfigPath: configPath,
		Plan:                plan,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not empty")
}

func TestWriteForkDatadir_AcceptsExistingEmptyTarget(t *testing.T) {
	parent := t.TempDir()
	target := t.TempDir() // empty
	configPath := filepath.Join(t.TempDir(), "chain.json")
	plan := &CopyPlan{}

	_, _, err := WriteForkDatadir(WriteForkDatadirOpts{
		ParentSnapDir:       parent,
		ForkSnapDir:         target,
		ForkChainConfig:     minimalForkChainConfig(),
		ForkChainConfigPath: configPath,
		Plan:                plan,
	})
	require.NoError(t, err)
}

func TestWriteForkDatadir_RejectsBadInputs(t *testing.T) {
	good := WriteForkDatadirOpts{
		ParentSnapDir:       t.TempDir(),
		ForkSnapDir:         filepath.Join(t.TempDir(), "fork"),
		ForkChainConfig:     minimalForkChainConfig(),
		ForkChainConfigPath: filepath.Join(t.TempDir(), "chain.json"),
		Plan:                &CopyPlan{},
	}

	cases := map[string]func(*WriteForkDatadirOpts){
		"empty ParentSnapDir":      func(o *WriteForkDatadirOpts) { o.ParentSnapDir = "" },
		"empty ForkSnapDir":        func(o *WriteForkDatadirOpts) { o.ForkSnapDir = "" },
		"identical parent/fork":    func(o *WriteForkDatadirOpts) { o.ForkSnapDir = o.ParentSnapDir },
		"nil chain.Config":         func(o *WriteForkDatadirOpts) { o.ForkChainConfig = nil },
		"nil plan":                 func(o *WriteForkDatadirOpts) { o.Plan = nil },
		"nonexistent parent":       func(o *WriteForkDatadirOpts) { o.ParentSnapDir = filepath.Join(t.TempDir(), "missing") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			opts := good
			mutate(&opts)
			_, _, err := WriteForkDatadir(opts)
			require.Error(t, err)
		})
	}
}

func TestCopyOneFile_PreservesMode(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "x"), []byte("hello"), 0o600))

	n, err := copyOneFile(src, dst, "x")
	require.NoError(t, err)
	require.Equal(t, int64(5), n)

	info, err := os.Stat(filepath.Join(dst, "x"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
		"copyOneFile must preserve source file mode")
}

func TestCopyOneFile_RefusesToOverwrite(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "x"), []byte("new"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "x"), []byte("existing"), 0o644))

	_, err := copyOneFile(src, dst, "x")
	require.Error(t, err, "O_EXCL must prevent silent overwrite")
}
