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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestVerifyPBTAcceptsSoundArtifacts(t *testing.T) {
	address := common.Address(bytes.Repeat([]byte{0x11}, length.Addr))
	var basic [eip8297.ValueLength]byte
	basic[eip8297.BasicDataNonceOffset+7] = 1
	basic[eip8297.BasicDataBalanceOffset+15] = 2
	slot := [32]byte{31: 1}
	var slotValue [eip8297.ValueLength]byte
	slotValue[31] = 2
	address32 := eip8297.RightAlign32(address[:])
	addressHash := common.Hash(blake3.Sum256(address32[:]))
	emptyCodeHash := eip8297.CodeHashValue(common.Hash{})
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash[:]},
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.HeaderStorageOffset+1), Value: slotValue[:]},
	}
	root := eip8297.StateRootWithHash(entries, pbtVerifyHash)
	var snapshot bytes.Buffer
	_, err := artifact.WriteSnapshotStream(&snapshot, func(yield func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := yield(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	}, func() (common.Hash, error) { return root, nil })
	require.NoError(t, err)
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimagesStreamWithScratch(&preimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		return yield(address, func(slotYield func([32]byte) error) error { return slotYield(slot) })
	}, t.TempDir()))
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithOpenExisting())
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	genesis := common.Hash{0x42}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chain.Config{ChainName: "mainnet"}))
	storageTrie := trie.NewInMemoryTrie(nil)
	storageTrie.Update(crypto.Keccak256(slot[:]), bytes.TrimLeft(slotValue[:], "\x00"))
	account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(2), Root: storageTrie.Hash(), CodeHash: accounts.EmptyCodeHash}
	accountsTrie := trie.NewInMemoryTrieRLPEncoded(nil)
	accountsTrie.Update(crypto.Keccak256(address[:]), account.RLP())
	header := &types.Header{Number: *uint256.NewInt(7), Root: accountsTrie.Hash()}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 7))
	require.NoError(t, tx.Commit())
	db.Close()
	snapshotPath := t.TempDir() + "/pbt-snapshot.bin"
	preimagesPath := t.TempDir() + "/framed.bin"
	require.NoError(t, os.WriteFile(snapshotPath, snapshot.Bytes(), 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes(), 0o644))
	require.NoError(t, verifyPBTFilesWithMaxCodeSize(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7, params.MaxCodeSizeAmsterdam, ""))
	corruptSnapshot := append([]byte(nil), snapshot.Bytes()...)
	corruptSnapshot[len(corruptSnapshot)-1] ^= 1
	require.NoError(t, os.WriteFile(snapshotPath, corruptSnapshot, 0o644))
	require.ErrorIs(t, verifyPBTFilesWithMaxCodeSize(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7, params.MaxCodeSizeAmsterdam, ""), errVerifyPBTInvalid)
	require.NoError(t, os.WriteFile(snapshotPath, snapshot.Bytes(), 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes()[:length.Addr+4], 0o644))
	require.ErrorIs(t, verifyPBTFilesWithMaxCodeSize(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7, params.MaxCodeSizeAmsterdam, ""), errVerifyPBTInvalid)
	var surplusPreimages bytes.Buffer
	surplusAddresses := []common.Address{address, {0x22}}
	sort.Slice(surplusAddresses, func(i, j int) bool {
		return bytes.Compare(crypto.Keccak256(surplusAddresses[i][:]), crypto.Keccak256(surplusAddresses[j][:])) < 0
	})
	require.NoError(t, artifact.WritePreimagesStreamWithScratch(&surplusPreimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		for _, surplusAddress := range surplusAddresses {
			slotYield := func(_ func([32]byte) error) error { return nil }
			if surplusAddress == address {
				slotYield = func(yieldSlot func([32]byte) error) error { return yieldSlot(slot) }
			}
			if err := yield(surplusAddress, slotYield); err != nil {
				return err
			}
		}
		return nil
	}, t.TempDir()))
	require.NoError(t, os.WriteFile(preimagesPath, surplusPreimages.Bytes(), 0o644))
	require.ErrorIs(t, verifyPBTFilesWithMaxCodeSize(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7, params.MaxCodeSizeAmsterdam, ""), errVerifyPBTInvalid)
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes(), 0o644))
	wrongRoot := &types.Header{Number: header.Number, Root: header.Root}
	wrongRoot.Root[0] ^= 1
	db = temporaltest.NewTestDB(t, dirs, temporaltest.WithOpenExisting())
	tx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdb.WriteHeader(tx, wrongRoot))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, wrongRoot.Hash(), 7))
	require.NoError(t, tx.Commit())
	db.Close()
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes(), 0o644))
	require.ErrorIs(t, verifyPBTFilesWithMaxCodeSize(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7, params.MaxCodeSizeAmsterdam, ""), errVerifyPBTInvalid)
}

func TestVerifyPBTAccountRejectsOversizedIntegerFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mpt-accounts.sorted")
	t.Run("nonce", func(t *testing.T) {
		var record pbtVerifyAccountRecord
		var err error
		require.NotPanics(t, func() {
			record, err = pbtVerifyAccount(artifact.Header{Nonce: make([]byte, 9)}, common.Hash{}, path)
		})
		require.Empty(t, record)
		require.ErrorIs(t, err, errPBTVerifyScratchIO)
		require.ErrorContains(t, err, path)
	})
	t.Run("code size", func(t *testing.T) {
		var record pbtVerifyAccountRecord
		var err error
		require.NotPanics(t, func() {
			record, err = pbtVerifyAccount(artifact.Header{CodeSize: make([]byte, 5)}, common.Hash{}, path)
		})
		require.Empty(t, record)
		require.ErrorIs(t, err, errPBTVerifyScratchIO)
		require.ErrorContains(t, err, path)
	})
	t.Run("balance", func(t *testing.T) {
		var record pbtVerifyAccountRecord
		var err error
		require.NotPanics(t, func() {
			record, err = pbtVerifyAccount(artifact.Header{Balance: make([]byte, 17)}, common.Hash{}, path)
		})
		require.Empty(t, record)
		require.ErrorIs(t, err, errPBTVerifyScratchIO)
		require.ErrorContains(t, err, path)
	})
	t.Run("account kind", func(t *testing.T) {
		var record pbtVerifyAccountRecord
		var err error
		require.NotPanics(t, func() {
			record, err = pbtVerifyAccount(artifact.Header{Kind: 3}, common.Hash{}, path)
		})
		require.Empty(t, record)
		require.ErrorIs(t, err, errPBTVerifyScratchIO)
		require.ErrorContains(t, err, path)
	})
}

func TestVerifyPBTWrapsMalformedMPTHeadersAsScratchIO(t *testing.T) {
	scratch := t.TempDir()
	paths := make([]string, 4)
	for i := range paths {
		paths[i] = filepath.Join(scratch, fmt.Sprintf("input-%d", i))
		require.NoError(t, os.WriteFile(paths[i], nil, 0o644))
	}
	require.NoError(t, os.WriteFile(paths[3], []byte{0xff}, 0o644))
	_, err := pbtVerifyMPT(paths[0], paths[1], paths[2], paths[3], scratch)
	require.ErrorIs(t, err, errPBTVerifyScratchIO)
}

func TestVerifyPBTRejectsMalformedArtifactWithoutPanic(t *testing.T) {
	snapshotPath := t.TempDir() + "/pbt-snapshot.bin"
	preimagesPath := t.TempDir() + "/framed.bin"
	require.NoError(t, os.WriteFile(snapshotPath, []byte{0x07}, 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, nil, 0o644))
	err := verifyPBTFilesWithMaxCodeSize(context.Background(), newHiveVerifyAnchor(t, common.Hash{}), snapshotPath, preimagesPath, 0, params.MaxCodeSizeAmsterdam, "")
	require.ErrorIs(t, err, errVerifyPBTInvalid)
}

func TestVerifyPBTAcceptsExportWithLeadingZeroCodeChunk(t *testing.T) {
	verifyPBTRealExportWithSharedCode(t, []byte{0, 1})
}

func TestVerifyPBTAcceptsExportWithShortFinalCodeChunk(t *testing.T) {
	code := append(bytes.Repeat([]byte{0x5b}, 31), 0, 0x5b, 0x5b)
	verifyPBTRealExportWithSharedCode(t, code)
}

func TestVerifyPBTAcceptsPostAmsterdamCodeSize(t *testing.T) {
	verifyPBTRealExportWithSharedCode(t, bytes.Repeat([]byte{0x5b}, 24*1024+1))
}

func TestVerifyPBTAcceptsCodeSizeConfiguredAboveDefault(t *testing.T) {
	selectPBTExportSuite(t)
	code := bytes.Repeat([]byte{0x5b}, 65_537)
	fixture, err := execmoduletester.NewPBTAcceptanceChainWithSharedCode(t, false, true, code)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	dirs := fixture.Tester.Dirs
	tx, err := fixture.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, runExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		if block == 0 {
			return fixture.Tester.Genesis.HeaderNoCopy(), nil
		}
		return fixture.Chain.Headers[block-1], nil
	}, outDir, log.New()))
	tx.Rollback()
	fixture.Tester.Close()
	snapshot := filepath.Join(outDir, pbtSnapshotFileName)
	preimages := filepath.Join(outDir, pbtPreimagesFileName)
	defaultErr, stderr := runVerifyPBTTestCommandAtBlock(t, dirs.DataDir, snapshot, preimages, 4)
	var defaultExit cli.ExitCoder
	require.ErrorAs(t, defaultErr, &defaultExit)
	require.Equal(t, 2, defaultExit.ExitCode())
	require.Contains(t, stderr, "--max-code-size")
	maxCodeSize := uint64(65_537)
	configuredErr, _ := runVerifyPBTTestCommandAtBlockWithMax(t, dirs.DataDir, snapshot, preimages, 4, maxCodeSize)
	require.NoError(t, configuredErr)
}

func TestVerifyPBTRejectsCodeSizeAboveConfiguredLimitBeforeExpansion(t *testing.T) {
	address := common.Address{1}
	address32 := eip8297.RightAlign32(address[:])
	addressHash := common.Hash(blake3.Sum256(address32[:]))
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(0), 65_537)
	require.NoError(t, err)
	codeHash := common.Hash{2}
	codeHashValue := eip8297.CodeHashValue(codeHash)
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.CodeHashLeafKey), Value: codeHashValue[:]},
	}
	root := eip8297.StateRootWithHash(entries, pbtVerifyHash)
	var snapshot bytes.Buffer
	_, err = artifact.WriteSnapshotStream(&snapshot, func(yield func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := yield(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	}, func() (common.Hash, error) { return root, nil })
	require.NoError(t, err)
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimagesStreamWithScratch(&preimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		return yield(address, func(func([32]byte) error) error { return nil })
	}, t.TempDir()))
	anchor := newHiveVerifyAnchor(t, common.Hash{})
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	preimagesPath := filepath.Join(t.TempDir(), "preimages.bin")
	require.NoError(t, os.WriteFile(snapshotPath, snapshot.Bytes(), 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes(), 0o644))
	scratch := t.TempDir()
	err = verifyPBTFilesWithMaxCodeSize(t.Context(), anchor, snapshotPath, preimagesPath, 0, 64*1024, scratch)
	require.ErrorIs(t, err, errVerifyPBTConfig)
	require.ErrorContains(t, err, "snapshot records:")
}

func verifyPBTRealExportWithSharedCode(t *testing.T, code []byte) {
	t.Helper()
	selectPBTExportSuite(t)
	fixture, err := execmoduletester.NewPBTAcceptanceChainWithSharedCode(t, false, true, code)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	dirs := fixture.Tester.Dirs
	tx, err := fixture.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, runExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		if block == 0 {
			return fixture.Tester.Genesis.HeaderNoCopy(), nil
		}
		return fixture.Chain.Headers[block-1], nil
	}, outDir, log.New()))
	tx.Rollback()
	fixture.Tester.Close()
	snapshot := filepath.Join(outDir, pbtSnapshotFileName)
	preimages := filepath.Join(outDir, pbtPreimagesFileName)
	validErr, _ := runVerifyPBTTestCommandAtBlock(t, dirs.DataDir, snapshot, preimages, 4)
	require.NoError(t, validErr)
	invalidErr, stderr := runVerifyPBTTestCommandAtBlock(t, dirs.DataDir, snapshot, preimages, 3)
	assertVerifyPBTRejected(t, invalidErr, stderr)
}

func TestVerifyPBTHiveFixtures(t *testing.T) {
	manifest := readHivePBTManifest(t)
	anchor := newHiveVerifyAnchor(t, common.HexToHash(manifest.Genesis.StateRoot))
	root := filepath.Join("testdata", "hive-pbt-fixtures")
	validSnapshot := filepath.Join(root, manifest.Valid.Snapshot)
	validPreimages := filepath.Join(root, manifest.Valid.Preimages)
	validErr, _ := runVerifyPBTTestCommand(t, anchor, validSnapshot, validPreimages)
	require.NoError(t, validErr)

	var preimageCount, snapshotCount, produceCount int
	for _, testCase := range manifest.Cases {
		if testCase.Suite == "produce" {
			produceCount++
			continue
		}
		t.Run(testCase.ID, func(t *testing.T) {
			snapshot := filepath.Join(root, testCase.Snapshot)
			preimages := filepath.Join(root, testCase.Preimages)
			err, stderr := runVerifyPBTTestCommand(t, anchor, snapshot, preimages)
			assertVerifyPBTRejected(t, err, stderr)
		})
		switch testCase.Suite {
		case "preimages":
			preimageCount++
		case "snapshot":
			snapshotCount++
		default:
			t.Fatalf("unknown hive fixture suite %q", testCase.Suite)
		}
	}
	require.NotEmpty(t, preimageCount)
	require.NotEmpty(t, snapshotCount)
	require.NotEmpty(t, produceCount)
}

type hivePBTManifest struct {
	Genesis struct {
		StateRoot string `json:"stateRoot"`
	} `json:"genesis"`
	Valid struct {
		Snapshot  string `json:"snapshot"`
		Preimages string `json:"preimages"`
	} `json:"valid"`
	Cases []struct {
		ID        string `json:"id"`
		Suite     string `json:"suite"`
		Snapshot  string `json:"snapshot"`
		Preimages string `json:"preimages"`
	} `json:"cases"`
}

func readHivePBTManifest(t *testing.T) hivePBTManifest {
	t.Helper()
	manifestBytes, err := os.ReadFile(filepath.Join("testdata", "hive-pbt-fixtures", "manifest.json"))
	require.NoError(t, err)
	var manifest hivePBTManifest
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	return manifest
}

func runVerifyPBTTestCommand(t *testing.T, anchor, snapshot, preimages string) (error, string) {
	return runVerifyPBTTestCommandAtBlock(t, anchor, snapshot, preimages, 0)
}

func runVerifyPBTTestCommandAtBlock(t *testing.T, anchor, snapshot, preimages string, block uint64) (error, string) {
	return runVerifyPBTTestCommandAtBlockWithMax(t, anchor, snapshot, preimages, block, 0)
}

func runVerifyPBTTestCommandAtBlockWithMax(t *testing.T, anchor, snapshot, preimages string, block, maxCodeSize uint64) (error, string) {
	t.Helper()
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "snapshot"},
		&cli.StringFlag{Name: "preimages"},
		&cli.Uint64Flag{Name: "block"},
		&cli.Uint64Flag{Name: "max-code-size", Value: 64 * 1024},
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, anchor))
	require.NoError(t, cmd.Set("snapshot", snapshot))
	require.NoError(t, cmd.Set("preimages", preimages))
	require.NoError(t, cmd.Set("block", fmt.Sprint(block)))
	if maxCodeSize != 0 {
		require.NoError(t, cmd.Set("max-code-size", fmt.Sprint(maxCodeSize)))
	}
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	runErr := doVerifyPBT(t.Context(), cmd)
	require.NoError(t, w.Close())
	os.Stderr = oldStderr
	data, readErr := io.ReadAll(r)
	require.NoError(t, readErr)
	require.NoError(t, r.Close())
	return runErr, string(data)
}

func assertVerifyPBTRejected(t *testing.T, err error, stderr string) {
	t.Helper()
	var exitErr cli.ExitCoder
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.NotEmpty(t, stderr)
}

func TestVerifyPBTUsesFrozenBlockFiles(t *testing.T) {
	selectPBTExportSuite(t)
	dirs := buildFrozenPBTExportDatadir(t)
	outDir := filepath.Join(t.TempDir(), "export")
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "out"},
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, dirs.DataDir))
	require.NoError(t, cmd.Set("out", outDir))
	require.NoError(t, doExportPBT(t.Context(), cmd))
	require.NoError(t, verifyPBTFilesWithMaxCodeSize(t.Context(), dirs.DataDir,
		filepath.Join(outDir, pbtSnapshotFileName),
		filepath.Join(outDir, pbtPreimagesFileName), 2, params.MaxCodeSizeAmsterdam, ""))
}

func TestVerifyPBTClassifiesSnapshotIO(t *testing.T) {
	manifest := readHivePBTManifest(t)
	anchor := newHiveVerifyAnchor(t, common.HexToHash(manifest.Genesis.StateRoot))
	root := filepath.Join("testdata", "hive-pbt-fixtures", "valid")
	scratch := filepath.Join(t.TempDir(), "scratch-file")
	require.NoError(t, os.WriteFile(scratch, nil, 0o644))
	err := verifyPBTFilesWithMaxCodeSize(t.Context(), anchor, filepath.Join(root, "snapshot.bin"), filepath.Join(root, "preimages.bin"), 0, params.MaxCodeSizeAmsterdam, scratch)
	require.Error(t, err)
	require.NotErrorIs(t, err, errVerifyPBTInvalid)
	err = verifyPBTFilesWithMaxCodeSize(t.Context(), anchor, filepath.Join(filepath.Dir(root), "snapshot"), filepath.Join(root, "preimages.bin"), 0, params.MaxCodeSizeAmsterdam, "")
	require.Error(t, err)
	require.NotErrorIs(t, err, errVerifyPBTInvalid)
	err = verifyPBTFilesWithMaxCodeSize(t.Context(), filepath.Join(t.TempDir(), "missing"), filepath.Join(root, "snapshot.bin"), filepath.Join(root, "preimages.bin"), 0, params.MaxCodeSizeAmsterdam, "")
	require.ErrorContains(t, err, "datadir does not exist")
	anchorDirs := datadir.Open(anchor)
	require.NoError(t, dir.RemoveAll(anchorDirs.Tmp))
	err = verifyPBTFilesWithMaxCodeSize(t.Context(), anchor, filepath.Join(root, "snapshot.bin"), filepath.Join(root, "preimages.bin"), 0, params.MaxCodeSizeAmsterdam, "")
	require.Error(t, err)
	_, statErr := os.Stat(anchorDirs.Tmp)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestVerifyPBTScratchRowDecodeIsIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "row")
	require.NoError(t, os.WriteFile(path, []byte{0, 0, 0, 8}, 0o644))
	file, reader, err := pbtVerifyOpenKV(path)
	require.NoError(t, err)
	defer file.Close()
	_, _, _, err = reader.next()
	require.ErrorIs(t, err, errPBTVerifyScratchIO)
}

func TestVerifyPBTScratchDecodeErrorsAreIO(t *testing.T) {
	for _, testCase := range []struct {
		name string
		call func(string) error
	}{
		{name: "mpt", call: func(path string) error {
			_, err := pbtVerifyHashMPT(path)
			return err
		}},
		{name: "account", call: func(path string) error {
			storagePath := filepath.Join(filepath.Dir(path), "storage.sorted")
			require.NoError(t, os.WriteFile(storagePath, nil, 0o644))
			collector := pbtVerifyNewCollector("scratch-decode-test", filepath.Dir(path))
			defer collector.Close()
			return pbtVerifyBuildAccountRows(path, storagePath, collector)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rows.sorted")
			file, err := os.Create(path)
			require.NoError(t, err)
			writer := bufio.NewWriter(file)
			require.NoError(t, pbtVerifyWriteKV(writer, bytes.Repeat([]byte{1}, length.Hash), []byte{0xff, 0xff, 0xff}))
			require.NoError(t, writer.Flush())
			require.NoError(t, file.Close())
			err = testCase.call(path)
			require.ErrorIs(t, err, errPBTVerifyScratchIO)
		})
	}
}

func TestMeasureVerifyPBTStreamingMemory(t *testing.T) {
	peaks := make([]uint64, 0, 2)
	for _, count := range []int{1000, 100000} {
		peak := measureVerifyPBTPeak(t, count)
		peaks = append(peaks, peak)
		t.Logf("accounts=%d live_heap_delta=%d", count, peak)
	}
	delta := uint64(0)
	if peaks[1] > peaks[0] {
		delta = peaks[1] - peaks[0]
	}
	require.Less(t, delta, uint64(16<<20), "streaming verifier retained heap must have a constant margin")
}

func measureVerifyPBTPeak(t *testing.T, count int) uint64 {
	t.Helper()
	snapshot, preimages, root := buildMeasuredPBT(t, count)
	anchor := newMeasuredPBTAnchor(t, root)
	runtime.GC()
	base := verifyPBTLiveHeap()
	require.NoError(t, verifyPBTFilesWithMaxCodeSize(t.Context(), anchor, snapshot, preimages, 0, params.MaxCodeSizeAmsterdam, ""))
	runtime.GC()
	used := verifyPBTLiveHeap()
	if used <= base {
		return 0
	}
	return used - base
}

func verifyPBTLiveHeap() uint64 {
	samples := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64()
}

func buildMeasuredPBT(t *testing.T, count int) (string, string, common.Hash) {
	t.Helper()
	entries := make([]eip8297.Entry, 0, count*3)
	addresses := make([]common.Address, count)
	for i := range addresses {
		addresses[i][length.Addr-4] = byte(i >> 24)
		addresses[i][length.Addr-3] = byte(i >> 16)
		addresses[i][length.Addr-2] = byte(i >> 8)
		addresses[i][length.Addr-1] = byte(i)
		address32 := eip8297.RightAlign32(addresses[i][:])
		addressHash := blake3.Sum256(address32[:])
		basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(0), 0)
		require.NoError(t, err)
		emptyCode := eip8297.CodeHashValue(common.Hash{})
		slotValue := make([]byte, eip8297.ValueLength)
		slotValue[len(slotValue)-1] = 1
		entries = append(entries,
			eip8297.Entry{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.BasicDataLeafKey), Value: basic[:]},
			eip8297.Entry{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.CodeHashLeafKey), Value: emptyCode[:]},
			eip8297.Entry{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.HeaderStorageOffset+1), Value: slotValue},
		)
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	root := eip8297.StateRootWithHash(entries, pbtVerifyHash)
	var snapshot bytes.Buffer
	_, err := artifact.WriteSnapshotStream(&snapshot, func(yield func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := yield(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	}, func() (common.Hash, error) { return root, nil })
	require.NoError(t, err)
	sort.Slice(addresses, func(i, j int) bool {
		return bytes.Compare(crypto.Keccak256(addresses[i][:]), crypto.Keccak256(addresses[j][:])) < 0
	})
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimagesStreamWithScratch(&preimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		for _, address := range addresses {
			if err := yield(address, func(yieldSlot func([32]byte) error) error { return yieldSlot([32]byte{31: 1}) }); err != nil {
				return err
			}
		}
		return nil
	}, t.TempDir()))
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	preimagesPath := filepath.Join(t.TempDir(), "preimages.bin")
	require.NoError(t, os.WriteFile(snapshotPath, snapshot.Bytes(), 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes(), 0o644))
	return snapshotPath, preimagesPath, measuredMPTRoot(addresses)
}

func measuredMPTRoot(addresses []common.Address) common.Hash {
	accountsTrie := trie.NewInMemoryTrieRLPEncoded(nil)
	for _, address := range addresses {
		storageTrie := trie.NewInMemoryTrie(nil)
		slot := [32]byte{31: 1}
		storageTrie.Update(crypto.Keccak256(slot[:]), []byte{1})
		account := accounts.Account{Nonce: 1, Root: storageTrie.Hash(), CodeHash: accounts.EmptyCodeHash}
		accountsTrie.Update(crypto.Keccak256(address[:]), account.RLP())
	}
	return accountsTrie.Hash()
}

func newMeasuredPBTAnchor(t *testing.T, root common.Hash) string {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithOpenExisting())
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	header := &types.Header{Number: *uint256.NewInt(0), Root: root}
	genesis := header.Hash()
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chain.Config{ChainName: "mainnet"}))
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, tx.Commit())
	db.Close()
	return dirs.DataDir
}

func TestVerifyPBTCommandPrintsRejection(t *testing.T) {
	manifest := readHivePBTManifest(t)
	anchor := newHiveVerifyAnchor(t, common.HexToHash(manifest.Genesis.StateRoot))
	root := filepath.Join("testdata", "hive-pbt-fixtures")
	snapshot := filepath.Join(t.TempDir(), "snapshot.bin")
	valid, err := os.ReadFile(filepath.Join(root, "valid", "snapshot.bin"))
	require.NoError(t, err)
	valid[len(valid)-1] ^= 1
	require.NoError(t, os.WriteFile(snapshot, valid, 0o644))
	preimages := filepath.Join(root, "valid", "preimages.bin")
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "snapshot"},
		&cli.StringFlag{Name: "preimages"},
		&cli.Uint64Flag{Name: "block"},
		&cli.Uint64Flag{Name: "max-code-size", Value: 64 * 1024},
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, anchor))
	require.NoError(t, cmd.Set("snapshot", snapshot))
	require.NoError(t, cmd.Set("preimages", preimages))
	require.NoError(t, cmd.Set("block", "0"))
	runErr := doVerifyPBT(t.Context(), cmd)
	require.NoError(t, w.Close())
	os.Stderr = oldStderr
	stderr, err := io.ReadAll(r)
	require.NoError(t, err)
	var exitErr cli.ExitCoder
	require.ErrorAs(t, runErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(stderr), "verify-pbt: artifact rejected")
}

func TestVerifyPBTCommandPrintsRejectionInFreshProcess(t *testing.T) {
	manifest := readHivePBTManifest(t)
	anchor := newHiveVerifyAnchor(t, common.HexToHash(manifest.Genesis.StateRoot))
	root := filepath.Join("testdata", "hive-pbt-fixtures")
	snapshot := filepath.Join(t.TempDir(), "snapshot.bin")
	valid, err := os.ReadFile(filepath.Join(root, "valid", "snapshot.bin"))
	require.NoError(t, err)
	valid[len(valid)-1] ^= 1
	require.NoError(t, os.WriteFile(snapshot, valid, 0o644))
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestVerifyPBTCommandHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_VERIFY_PBT_HELPER=1",
		"VERIFY_PBT_DATADIR="+anchor,
		"VERIFY_PBT_SNAPSHOT="+snapshot,
		"VERIFY_PBT_PREIMAGES="+filepath.Join(root, "valid", "preimages.bin"),
	)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(output), "verify-pbt: artifact rejected")
}

func TestVerifyPBTUsageErrorsExitTwoInFreshProcess(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		stderr string
	}{
		{name: "missing-snapshot", stderr: "--snapshot, --preimages and --block are required"},
		{name: "missing-preimages", stderr: "--snapshot, --preimages and --block are required"},
		{name: "missing-block", stderr: "--snapshot, --preimages and --block are required"},
		{name: "positional", stderr: "unexpected positional arguments"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestVerifyPBTUsageHelperProcess$", "-test.v")
			command.Env = append(os.Environ(), "GO_WANT_VERIFY_PBT_USAGE_HELPER=1", "VERIFY_PBT_USAGE_CASE="+testCase.name)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr)
			require.Equal(t, 2, exitErr.ExitCode())
			require.Contains(t, string(output), testCase.stderr)
		})
	}
}

func TestVerifyPBTUsageHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_VERIFY_PBT_USAGE_HELPER") != "1" {
		return
	}
	var args []string
	switch os.Getenv("VERIFY_PBT_USAGE_CASE") {
	case "missing-snapshot":
		args = []string{"--preimages=preimages", "--block=0"}
	case "missing-preimages":
		args = []string{"--snapshot=snapshot", "--block=0"}
	case "missing-block":
		args = []string{"--snapshot=snapshot", "--preimages=preimages"}
	case "positional":
		args = []string{"--snapshot=snapshot", "--preimages=preimages", "--block=0", "extra"}
	default:
		os.Exit(3)
	}
	if err := verifyPBTCommand.Run(t.Context(), append([]string{"verify-pbt"}, args...)); err != nil {
		if exitErr, ok := errors.AsType[cli.ExitCoder](err); ok {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	os.Exit(0)
}

func TestVerifyPBTCommandHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_VERIFY_PBT_HELPER") != "1" {
		return
	}
	command := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "snapshot"},
		&cli.StringFlag{Name: "preimages"},
		&cli.Uint64Flag{Name: "block"},
		&cli.Uint64Flag{Name: "max-code-size", Value: 64 * 1024},
	}}
	require.NoError(t, command.Set(utils.DataDirFlag.Name, os.Getenv("VERIFY_PBT_DATADIR")))
	require.NoError(t, command.Set("snapshot", os.Getenv("VERIFY_PBT_SNAPSHOT")))
	require.NoError(t, command.Set("preimages", os.Getenv("VERIFY_PBT_PREIMAGES")))
	require.NoError(t, command.Set("block", "0"))
	err := doVerifyPBT(t.Context(), command)
	if err == nil {
		os.Exit(0)
	}
	if exitErr, ok := errors.AsType[cli.ExitCoder](err); ok {
		os.Exit(exitErr.ExitCode())
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

func newHiveVerifyAnchor(t *testing.T, root common.Hash) string {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithOpenExisting())
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	header := &types.Header{Number: *uint256.NewInt(0), Root: root}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, header.Hash(), &chain.Config{ChainName: "mainnet"}))
	require.NoError(t, tx.Commit())
	db.Close()
	return dirs.DataDir
}
