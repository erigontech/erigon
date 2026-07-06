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
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/tests/testforks"
	erigoncli "github.com/erigontech/erigon/node/cli"
)

type importFixtureCase struct {
	LastBlockHash string                     `json:"lastblockhash"`
	GenesisHeader map[string]json.RawMessage `json:"genesisBlockHeader"`
	Pre           json.RawMessage            `json:"pre"`
	Blocks        []struct {
		Rlp string `json:"rlp"`
	} `json:"blocks"`
}

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

	tc := loadImportFixtureCase(t)

	work := t.TempDir()
	genesisPath := writeImportGenesis(t, work, tc)
	rlpFiles := writeImportBlocks(t, work, tc)
	dataDir := t.TempDir()

	// --log.dir.disable keeps the process-wide root logger from holding a log
	// file under dataDir open past the commands, which would break t.TempDir
	// cleanup on Windows.
	require.NoError(t, runErigonCommand("--log.dir.disable", "init", "--datadir", dataDir, genesisPath))

	// The fixture's next-to-last block is an intentionally invalid post-merge
	// uncle: multi-file import skips the failed file, imports the final valid
	// block 4, and returns the retained rejection error.
	importArgs := append([]string{"--log.dir.disable", "import", "--datadir", dataDir}, rlpFiles...)
	importErr := runErigonCommand(importArgs...)

	var genesisHash string
	require.NoError(t, json.Unmarshal(tc.GenesisHeader["hash"], &genesisHash))

	ctx := context.Background()
	db, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(filepath.Join(dataDir, "chaindata")).Accede(true).Readonly(true).Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	var storedGenesis, head common.Hash
	var headNumber *uint64
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		var err error
		if storedGenesis, err = rawdb.ReadCanonicalHash(tx, 0); err != nil {
			return err
		}
		head = rawdb.ReadHeadBlockHash(tx)
		headNumber = rawdb.ReadHeaderNumber(tx, head)
		return nil
	}))
	require.Equal(t, genesisHash, storedGenesis.Hex(), "genesis hash mismatch — chain config drift?")
	require.ErrorContains(t, importErr, "uncle")
	require.NotNilf(t, headNumber, "no canonical head; import err: %v", importErr)
	require.Equalf(t, uint64(4), *headNumber,
		"head did not advance to the heavier side chain (block 4); import err: %v", importErr)
	require.Equalf(t, tc.LastBlockHash, head.Hex(),
		"final head mismatch (import err: %v)", importErr)
}

// TestImportClosesChaindataOnInitError makes ethereum.Init fail after eth.New
// has opened chaindata (malformed --ethstats URL) and asserts the import
// command still stops the backend: the in-process reopen below only succeeds
// if chaindata was closed on the error path.
func TestImportClosesChaindataOnInitError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	tc := loadImportFixtureCase(t)
	work := t.TempDir()
	genesisPath := writeImportGenesis(t, work, tc)
	rlpFiles := writeImportBlocks(t, work, tc)
	dataDir := t.TempDir()

	require.NoError(t, runErigonCommand("--log.dir.disable", "init", "--datadir", dataDir, genesisPath))

	importErr := runErigonCommand("--log.dir.disable", "--ethstats", "invalid",
		"import", "--datadir", dataDir, rlpFiles[0])
	require.ErrorContains(t, importErr, "netstats")

	db, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(filepath.Join(dataDir, "chaindata")).Accede(true).Readonly(true).Open(context.Background())
	require.NoError(t, err, "chaindata still open after failed import — backend not stopped on Init error?")
	db.Close()
}

func loadImportFixtureCase(t *testing.T) importFixtureCase {
	t.Helper()
	fixturePath := filepath.Join("..", "..", "..", "execution", "tests", "legacy-tests",
		"BlockchainTests", "InvalidBlocks", "bcMultiChainTest", "UncleFromSideChain.json")
	raw, err := os.ReadFile(fixturePath)
	require.NoErrorf(t, err, "read fixture (legacy-tests submodule not initialized?)")

	var fixture map[string]importFixtureCase
	require.NoError(t, json.Unmarshal(raw, &fixture))
	const caseKey = "BlockchainTests/InvalidBlocks/bcMultiChainTest/UncleFromSideChain.json::UncleFromSideChain_Cancun"
	tc, ok := fixture[caseKey]
	require.Truef(t, ok, "case %q not found in fixture", caseKey)
	require.NotEmpty(t, tc.Blocks)
	return tc
}

func writeImportGenesis(t *testing.T, dir string, tc importFixtureCase) string {
	t.Helper()
	// Reuse the chain config the in-process fixture tests resolve so the CLI
	// genesis can't drift from it; the genesis-hash assertion guards drift.
	configJSON, err := json.Marshal(testforks.Forks["Cancun"])
	require.NoError(t, err)

	// The fixture's genesisBlockHeader fields are the genesis.json fields;
	// erigon ignores the block-only extras (stateRoot, bloom, ...).
	genesis := maps.Clone(tc.GenesisHeader)
	genesis["config"] = configJSON
	genesis["alloc"] = tc.Pre

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
		data, err := hexutil.Decode(b.Rlp)
		require.NoErrorf(t, err, "decode block %d rlp", i+1)
		p := filepath.Join(dir, fmt.Sprintf("%04d.rlp", i+1))
		require.NoError(t, os.WriteFile(p, data, 0o644))
		rlpFiles = append(rlpFiles, p)
	}
	return rlpFiles
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
