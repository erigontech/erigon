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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync/atomic"
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
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/commitment/trie"
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
	_, err := artifact.WriteSnapshot(&snapshot, root, func(yield func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := yield(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimagesStream(&preimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		return yield(address, func(slotYield func([32]byte) error) error { return slotYield(slot) })
	}))
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
	require.NoError(t, verifyPBTFiles(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7))
	corruptSnapshot := append([]byte(nil), snapshot.Bytes()...)
	corruptSnapshot[len(corruptSnapshot)-1] ^= 1
	require.NoError(t, os.WriteFile(snapshotPath, corruptSnapshot, 0o644))
	require.ErrorIs(t, verifyPBTFiles(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7), errVerifyPBTInvalid)
	require.NoError(t, os.WriteFile(snapshotPath, snapshot.Bytes(), 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes()[:length.Addr+4], 0o644))
	require.ErrorIs(t, verifyPBTFiles(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7), errVerifyPBTInvalid)
	var surplusPreimages bytes.Buffer
	surplusAddresses := []common.Address{address, {0x22}}
	sort.Slice(surplusAddresses, func(i, j int) bool {
		return bytes.Compare(crypto.Keccak256(surplusAddresses[i][:]), crypto.Keccak256(surplusAddresses[j][:])) < 0
	})
	require.NoError(t, artifact.WritePreimagesStream(&surplusPreimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
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
	}))
	require.NoError(t, os.WriteFile(preimagesPath, surplusPreimages.Bytes(), 0o644))
	require.ErrorIs(t, verifyPBTFiles(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7), errVerifyPBTInvalid)
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
	require.ErrorIs(t, verifyPBTFiles(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 7), errVerifyPBTInvalid)
}

func TestVerifyPBTRejectsMalformedArtifactWithoutPanic(t *testing.T) {
	snapshotPath := t.TempDir() + "/pbt-snapshot.bin"
	preimagesPath := t.TempDir() + "/framed.bin"
	require.NoError(t, os.WriteFile(snapshotPath, []byte{0x07}, 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, nil, 0o644))
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
	err := verifyPBTFiles(context.Background(), dirs.DataDir, snapshotPath, preimagesPath, 0)
	require.ErrorIs(t, err, errVerifyPBTInvalid)
}

func TestVerifyPBTRejectsOversizedCodeDeclaration(t *testing.T) {
	scratch := t.TempDir()
	requirements := filepath.Join(scratch, "requirements")
	expected := filepath.Join(scratch, "expected")
	f, err := os.Create(requirements)
	require.NoError(t, err)
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], ^uint64(0))
	w := bufio.NewWriter(f)
	require.NoError(t, pbtVerifyWriteKV(w, make([]byte, length.Hash), value[:]))
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
	require.ErrorContains(t, pbtVerifyGenerateCodeExpected(requirements, expected, scratch), "EIP maximum")
}

func TestVerifyPBTHiveFixtures(t *testing.T) {
	anchor := newHiveVerifyAnchor(t)
	root := filepath.Join("testdata", "hive-pbt-fixtures")
	validSnapshot := filepath.Join(root, "valid", "snapshot.bin")
	validPreimages := filepath.Join(root, "valid", "preimages.bin")
	validErr, _ := runVerifyPBTTestCommand(t, anchor, validSnapshot, validPreimages)
	require.NoError(t, validErr)

	preimageCases, err := os.ReadDir(filepath.Join(root, "preimages"))
	require.NoError(t, err)
	require.Len(t, preimageCases, 13)
	for _, entry := range preimageCases {
		t.Run(entry.Name(), func(t *testing.T) {
			err, stderr := runVerifyPBTTestCommand(t, anchor, validSnapshot, filepath.Join(root, "preimages", entry.Name(), "preimages.bin"))
			assertVerifyPBTRejected(t, err, stderr)
		})
	}
	snapshotCases, err := os.ReadDir(filepath.Join(root, "snapshot"))
	require.NoError(t, err)
	require.Len(t, snapshotCases, 53)
	for _, entry := range snapshotCases {
		t.Run(entry.Name(), func(t *testing.T) {
			preimages := validPreimages
			if entry.Name() == "anchored-elsewhere" {
				preimages = filepath.Join(root, "snapshot", entry.Name(), "preimages.bin")
			}
			err, stderr := runVerifyPBTTestCommand(t, anchor, filepath.Join(root, "snapshot", entry.Name(), "snapshot.bin"), preimages)
			assertVerifyPBTRejected(t, err, stderr)
		})
	}
}

func runVerifyPBTTestCommand(t *testing.T, anchor, snapshot, preimages string) (error, string) {
	t.Helper()
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "snapshot"},
		&cli.StringFlag{Name: "preimages"},
		&cli.Uint64Flag{Name: "block"},
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, anchor))
	require.NoError(t, cmd.Set("snapshot", snapshot))
	require.NoError(t, cmd.Set("preimages", preimages))
	require.NoError(t, cmd.Set("block", "0"))
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
	require.NoError(t, verifyPBTFiles(t.Context(), dirs.DataDir,
		filepath.Join(outDir, pbtSnapshotFileName),
		filepath.Join(outDir, pbtPreimagesFileName), 2))
}

func TestVerifyPBTClassifiesSnapshotIO(t *testing.T) {
	anchor := newHiveVerifyAnchor(t)
	root := filepath.Join("testdata", "hive-pbt-fixtures", "valid")
	scratch := filepath.Join(t.TempDir(), "scratch-file")
	require.NoError(t, os.WriteFile(scratch, nil, 0o644))
	err := verifyPBTFiles(t.Context(), anchor, filepath.Join(root, "snapshot.bin"), filepath.Join(root, "preimages.bin"), 0, scratch)
	require.Error(t, err)
	require.NotErrorIs(t, err, errVerifyPBTInvalid)
	err = verifyPBTFiles(t.Context(), anchor, filepath.Join(filepath.Dir(root), "snapshot"), filepath.Join(root, "preimages.bin"), 0)
	require.Error(t, err)
	require.NotErrorIs(t, err, errVerifyPBTInvalid)
	err = verifyPBTFiles(t.Context(), filepath.Join(t.TempDir(), "missing"), filepath.Join(root, "snapshot.bin"), filepath.Join(root, "preimages.bin"), 0)
	require.ErrorContains(t, err, "datadir does not exist")
	anchorDirs := datadir.Open(anchor)
	require.NoError(t, dir.RemoveAll(anchorDirs.Tmp))
	err = verifyPBTFiles(t.Context(), anchor, filepath.Join(root, "snapshot.bin"), filepath.Join(root, "preimages.bin"), 0)
	require.Error(t, err)
	_, statErr := os.Stat(anchorDirs.Tmp)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestMeasureVerifyPBTStreamingMemory(t *testing.T) {
	peaks := make([]uint64, 0, 2)
	for _, count := range []int{1000, 5000} {
		peak := measureVerifyPBTPeak(t, count)
		peaks = append(peaks, peak)
		t.Logf("accounts=%d peak_heap_stack=%d", count, peak)
	}
	require.Less(t, peaks[1], peaks[0]*2, "streaming verifier memory must not scale with account count")
}

func measureVerifyPBTPeak(t *testing.T, count int) uint64 {
	t.Helper()
	snapshot, preimages, root := buildMeasuredPBT(t, count)
	anchor := newMeasuredPBTAnchor(t, root)
	var peak atomic.Uint64
	done := make(chan struct{})
	go func() {
		var stats runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
			}
			runtime.ReadMemStats(&stats)
			used := stats.HeapInuse + stats.StackInuse
			for {
				old := peak.Load()
				if used <= old || peak.CompareAndSwap(old, used) {
					break
				}
			}
		}
	}()
	stop := func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	t.Cleanup(stop)
	require.NoError(t, verifyPBTFiles(t.Context(), anchor, snapshot, preimages, 0))
	stop()
	return peak.Load()
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
	_, err := artifact.WriteSnapshot(&snapshot, root, func(yield func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := yield(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	sort.Slice(addresses, func(i, j int) bool {
		return bytes.Compare(crypto.Keccak256(addresses[i][:]), crypto.Keccak256(addresses[j][:])) < 0
	})
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimagesStream(&preimages, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		for _, address := range addresses {
			if err := yield(address, func(yieldSlot func([32]byte) error) error { return yieldSlot([32]byte{31: 1}) }); err != nil {
				return err
			}
		}
		return nil
	}))
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
	anchor := newHiveVerifyAnchor(t)
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
	anchor := newHiveVerifyAnchor(t)
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

func TestVerifyPBTCommandHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_VERIFY_PBT_HELPER") != "1" {
		return
	}
	command := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "snapshot"},
		&cli.StringFlag{Name: "preimages"},
		&cli.Uint64Flag{Name: "block"},
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

func newHiveVerifyAnchor(t *testing.T) string {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithOpenExisting())
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	root := common.HexToHash("0xb5656f58d12a56427942e7a422755496122c7fceba8b3af277f0df13da57d50c")
	header := &types.Header{Number: *uint256.NewInt(0), Root: root}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, header.Hash(), &chain.Config{ChainName: "mainnet"}))
	require.NoError(t, tx.Commit())
	db.Close()
	return dirs.DataDir
}
