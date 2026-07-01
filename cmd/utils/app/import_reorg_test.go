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

package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/execution/tests/testforks"
	erigoncli "github.com/erigontech/erigon/node/cli"
)

type importFixtureCase struct {
	LastBlockHash string `json:"lastblockhash"`
	GenesisHeader struct {
		Hash          string `json:"hash"`
		Nonce         string `json:"nonce"`
		Timestamp     string `json:"timestamp"`
		ExtraData     string `json:"extraData"`
		GasLimit      string `json:"gasLimit"`
		GasUsed       string `json:"gasUsed"`
		Difficulty    string `json:"difficulty"`
		MixHash       string `json:"mixHash"`
		Coinbase      string `json:"coinbase"`
		ParentHash    string `json:"parentHash"`
		BaseFeePerGas string `json:"baseFeePerGas"`
		ExcessBlobGas string `json:"excessBlobGas"`
		Number        string `json:"number"`
	} `json:"genesisBlockHeader"`
	Pre    map[string]json.RawMessage `json:"pre"`
	Blocks []struct {
		Rlp string `json:"rlp"`
	} `json:"blocks"`
}

var headUpdatedRe = regexp.MustCompile(`head updated\s+hash=(0x[0-9a-fA-F]{64})\s+number=(\d+)`)

// TestImportReorgUnwindToGenesis drives the real erigon init + import commands
// in-process for a chain that reorgs back to genesis, asserting the canonical
// head advances to the heavier side chain. Running the actual commands is what
// exercises the parallel-executor genesis-commitment path this guards: the
// in-process block-test harness commits genesis through a different path that
// doesn't hit the regression.
func TestImportReorgUnwindToGenesis(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	root := repoRootDir(t)
	fixturePath := filepath.Join(root, "execution", "tests", "legacy-tests",
		"BlockchainTests", "InvalidBlocks", "bcMultiChainTest", "UncleFromSideChain.json")
	raw, err := os.ReadFile(fixturePath)
	require.NoError(t, err)

	var fixture map[string]importFixtureCase
	require.NoError(t, json.Unmarshal(raw, &fixture))
	const caseKey = "BlockchainTests/InvalidBlocks/bcMultiChainTest/UncleFromSideChain.json::UncleFromSideChain_Cancun"
	tc, ok := fixture[caseKey]
	require.Truef(t, ok, "case %q not found in fixture", caseKey)
	require.NotEmpty(t, tc.Blocks)

	work := t.TempDir()
	genesisPath := writeImportGenesis(t, work, tc)
	rlpFiles := writeImportBlocks(t, work, tc)

	// The import command leaves the chaindata open until process exit, which
	// blocks t.TempDir's RemoveAll on Windows (an open file can't be deleted),
	// so keep the datadir outside it and clean up best-effort.
	dataDir, err := os.MkdirTemp("", "erigon-import-reorg-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.RemoveAll(dataDir) })

	require.NoError(t, runErigonCommand("init", "--datadir", dataDir, genesisPath))

	// Import returns an error by design: the fixture ends with an intentionally
	// invalid block (a post-merge uncle). Assert on the canonical head reached,
	// not the error; a genuine startup failure surfaces as the head never
	// advancing, with importErr reported in the failure message.
	importArgs := append([]string{
		"--http=false",
		"--private.api.addr=",
		"--authrpc.port=0",
		"import", "--datadir", dataDir, "--networkid", "1337",
	}, rlpFiles...)
	importErr := runErigonCommand(importArgs...)

	// The import command leaves the chaindata open until process exit, so assert
	// on erigon's own log file (init and import both append to it) rather than
	// reopening the DB.
	logs, err := os.ReadFile(filepath.Join(dataDir, "logs", "erigon.log"))
	require.NoError(t, err)
	require.Containsf(t, string(logs), tc.GenesisHeader.Hash,
		"genesis hash mismatch — chain config drift?")

	head, number := lastImportedHead(t, string(logs))
	require.Equalf(t, uint64(4), number,
		"head did not advance to the heavier side chain (block 4); import err: %v", importErr)
	require.Equalf(t, tc.LastBlockHash, head,
		"final head mismatch (import err: %v)", importErr)
}

func writeImportGenesis(t *testing.T, dir string, tc importFixtureCase) string {
	t.Helper()
	alloc := make(map[string]json.RawMessage, len(tc.Pre))
	for addr, acc := range tc.Pre {
		alloc[strings.TrimPrefix(addr, "0x")] = acc
	}
	// Reuse the chain config the in-process fixture tests resolve so the CLI
	// genesis can't silently drift from it; the genesis-hash assertion still
	// guards header/alloc drift.
	configJSON, err := json.Marshal(testforks.Forks["Cancun"])
	require.NoError(t, err)
	genesis := map[string]any{
		"config":        json.RawMessage(configJSON),
		"nonce":         tc.GenesisHeader.Nonce,
		"timestamp":     tc.GenesisHeader.Timestamp,
		"extraData":     tc.GenesisHeader.ExtraData,
		"gasLimit":      tc.GenesisHeader.GasLimit,
		"gasUsed":       tc.GenesisHeader.GasUsed,
		"difficulty":    tc.GenesisHeader.Difficulty,
		"mixHash":       tc.GenesisHeader.MixHash,
		"coinbase":      tc.GenesisHeader.Coinbase,
		"parentHash":    tc.GenesisHeader.ParentHash,
		"baseFeePerGas": tc.GenesisHeader.BaseFeePerGas,
		"excessBlobGas": tc.GenesisHeader.ExcessBlobGas,
		"number":        tc.GenesisHeader.Number,
		"alloc":         alloc,
	}
	genesisJSON, err := json.Marshal(genesis)
	require.NoError(t, err)
	genesisPath := filepath.Join(dir, "genesis.json")
	require.NoError(t, os.WriteFile(genesisPath, genesisJSON, 0o644))
	return genesisPath
}

func writeImportBlocks(t *testing.T, dir string, tc importFixtureCase) []string {
	t.Helper()
	rlpFiles := make([]string, 0, len(tc.Blocks))
	for i, b := range tc.Blocks {
		data, err := hex.DecodeString(strings.TrimPrefix(b.Rlp, "0x"))
		require.NoErrorf(t, err, "decode block %d rlp", i+1)
		p := filepath.Join(dir, fmt.Sprintf("%04d.rlp", i+1))
		require.NoError(t, os.WriteFile(p, data, 0o644))
		rlpFiles = append(rlpFiles, p)
	}
	return rlpFiles
}

// lastImportedHead returns the hash and number of the final canonical head the
// import settled on, from the last "head updated" log line.
func lastImportedHead(t *testing.T, logs string) (hash string, number uint64) {
	t.Helper()
	matches := headUpdatedRe.FindAllStringSubmatch(logs, -1)
	require.NotEmpty(t, matches, "no 'head updated' line found in import logs")
	last := matches[len(matches)-1]
	number, err := strconv.ParseUint(last[2], 10, 64)
	require.NoError(t, err)
	return last[1], number
}

// runErigonCommand runs the erigon CLI app in-process — the same app the binary
// builds — so init/import exercise the production command path. The no-op
// ExitErrHandler makes a failing command return its error instead of calling
// os.Exit, so import's expected invalid-block error is observable.
func runErigonCommand(args ...string) error {
	app := MakeApp("erigon", func(*cli.Context) error { return nil }, erigoncli.DefaultFlags)
	app.ExitErrHandler = func(*cli.Context, error) {}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return app.RunContext(ctx, append([]string{"erigon"}, args...))
}

// repoRootDir walks up from this source file to the module root (the dir with go.mod).
func repoRootDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqualf(t, parent, dir, "go.mod not found above %s", file)
		dir = parent
	}
}
