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
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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
	err := verifyPBTFiles(context.Background(), t.TempDir(), snapshotPath, preimagesPath, 0)
	require.ErrorIs(t, err, errVerifyPBTInvalid)
}

func TestVerifyPBTChecksCodeChunks(t *testing.T) {
	address := common.Address(bytes.Repeat([]byte{0x21}, length.Addr))
	code := []byte{0x60, 0x01, 0x60, 0x02}
	codeHash := crypto.Keccak256Hash(code)
	address32 := eip8297.RightAlign32(address[:])
	addressHash := common.Hash(blake3.Sum256(address32[:]))
	var basic [eip8297.ValueLength]byte
	basic[eip8297.BasicDataCodeSizeOffset+3] = byte(len(code))
	var chunk [eip8297.ValueLength]byte
	copy(chunk[1:], code)
	var codeCache eip8297.DigestCache
	codeCache.Sum = pbtVerifyHash
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKey(eip8297.AccountZone, addressHash[:], eip8297.CodeHashLeafKey), Value: codeHash[:]},
		{Key: codeCache.CodeChunkKey(codeHash, 0), Value: chunk[:]},
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
		return yield(address, func(func([32]byte) error) error { return nil })
	}))
	state, err := readPBTVerificationState(bytes.NewReader(snapshot.Bytes()), int64(snapshot.Len()))
	require.NoError(t, err)
	require.NoError(t, readPBTVerificationPreimages(bytes.NewReader(preimages.Bytes()), int64(preimages.Len()), state))
	require.NoError(t, verifyPBTCode(state))
	for key := range state.code {
		state.code[key][0] ^= 1
		break
	}
	require.Error(t, verifyPBTCode(state))
}

func TestVerifyPBTHiveFixtures(t *testing.T) {
	anchor := newHiveVerifyAnchor(t)
	root := filepath.Join("testdata", "hive-pbt-fixtures")
	validSnapshot := filepath.Join(root, "valid", "snapshot.bin")
	validPreimages := filepath.Join(root, "valid", "preimages.bin")
	require.NoError(t, verifyPBTFiles(t.Context(), anchor, validSnapshot, validPreimages, 0))

	preimageCases, err := os.ReadDir(filepath.Join(root, "preimages"))
	require.NoError(t, err)
	require.Len(t, preimageCases, 13)
	for _, entry := range preimageCases {
		t.Run(entry.Name(), func(t *testing.T) {
			err := verifyPBTFiles(t.Context(), anchor, validSnapshot, filepath.Join(root, "preimages", entry.Name(), "preimages.bin"), 0)
			require.ErrorIs(t, err, errVerifyPBTInvalid)
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
			err := verifyPBTFiles(t.Context(), anchor, filepath.Join(root, "snapshot", entry.Name(), "snapshot.bin"), preimages, 0)
			require.ErrorIs(t, err, errVerifyPBTInvalid)
		})
	}
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
