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

package executiontests

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/tests/testforks"
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

// lastImportedHead returns the final canonical head the import settled on (the
// last head-update logged), or ("", 0) if none was logged.
func lastImportedHead(out string) (hash string, number uint64) {
	matches := headUpdatedRe.FindAllStringSubmatch(out, -1)
	if len(matches) == 0 {
		return "", 0
	}
	last := matches[len(matches)-1]
	number, _ = strconv.ParseUint(last[2], 10, 64)
	return last[1], number
}

// TestImportReorgUnwindToGenesis drives the real erigon init + import binary
// flow for a chain that reorgs back to genesis, asserting the canonical head
// advances to the heavier side chain. It must use the binary rather than the
// in-process block-test harness: that harness commits genesis through a
// separate path that doesn't hit this parallel-executor regression.
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

	alloc := make(map[string]json.RawMessage, len(tc.Pre))
	for addr, acc := range tc.Pre {
		alloc[strings.TrimPrefix(addr, "0x")] = acc
	}
	// Reuse the same chain config the in-process fixture tests resolve, so the
	// CLI genesis can't silently drift from it; the genesis-hash check below
	// still guards header/alloc drift.
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
	genesisPath := filepath.Join(work, "genesis.json")
	require.NoError(t, os.WriteFile(genesisPath, genesisJSON, 0o644))

	rlpFiles := make([]string, 0, len(tc.Blocks))
	for i, b := range tc.Blocks {
		data, err := hex.DecodeString(strings.TrimPrefix(b.Rlp, "0x"))
		require.NoErrorf(t, err, "decode block %d rlp", i+1)
		p := filepath.Join(work, fmt.Sprintf("%04d.rlp", i+1))
		require.NoError(t, os.WriteFile(p, data, 0o644))
		rlpFiles = append(rlpFiles, p)
	}

	bin := erigonBinaryForTest(t, root)
	datadir := filepath.Join(work, "datadir")

	initOut, err := exec.Command(bin, "init", "--datadir", datadir, genesisPath).CombinedOutput() //nolint:gosec
	require.NoErrorf(t, err, "erigon init failed:\n%s", initOut)
	require.Containsf(t, string(initOut), tc.GenesisHeader.Hash,
		"genesis hash mismatch — chain config drift?\n%s", initOut)

	// Import exits non-zero by design: the fixture includes an intentionally
	// invalid block (a post-merge uncle). We assert on the canonical head the
	// import reached, not the exit code.
	importArgs := append([]string{"import", "--datadir", datadir, "--networkid", "1337"}, rlpFiles...)
	importOut, importErr := exec.Command(bin, importArgs...).CombinedOutput() //nolint:gosec
	var exitErr *exec.ExitError
	if importErr != nil && !errors.As(importErr, &exitErr) {
		t.Fatalf("erigon import failed to run: %v\n%s", importErr, importOut)
	}

	head, number := lastImportedHead(string(importOut))
	require.Equalf(t, uint64(4), number,
		"head did not advance to the heavier side chain (block 4); reached block %d\n%s", number, importOut)
	require.Equalf(t, tc.LastBlockHash, head,
		"final head mismatch: want %s, have %s\n%s", tc.LastBlockHash, head, importOut)
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

// erigonBinaryForTest resolves the erigon binary to drive: $ERIGON_BIN (set by
// CI), else a prebuilt build/bin/erigon, else a slow local from-scratch build
// (which is why the test is skipped under -short).
func erigonBinaryForTest(t *testing.T, root string) string {
	t.Helper()
	if b := os.Getenv("ERIGON_BIN"); b != "" {
		return b
	}
	binPath := filepath.Join(root, "build", "bin", "erigon")
	if _, err := os.Stat(binPath); err == nil {
		return binPath
	}
	// Point go build's work dir (GOTMPDIR) at real disk under build/ rather
	// than the default temp: the from-scratch erigon link is large and can
	// overflow a small or tmpfs /tmp.
	buildRoot := filepath.Join(root, "build")
	require.NoError(t, os.MkdirAll(filepath.Join(buildRoot, "bin"), 0o755))
	build := exec.Command("go", "build", "-o", binPath, "./cmd/erigon") //nolint:gosec
	build.Dir = root
	build.Env = append(os.Environ(), "GOTMPDIR="+buildRoot)
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build erigon: %v\n%s", err, b)
	}
	return binPath
}
