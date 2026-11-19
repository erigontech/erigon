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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

func TestImportFilesProcessesEveryFile(t *testing.T) {
	files := []string{"0001.rlp", "0002.rlp", "0003.rlp"}
	var imported []string
	err := importFiles(files, log.Root(), func(fn string) error {
		imported = append(imported, fn)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, files, imported)
}

func TestImportFilesContinuesPastPerFileFailure(t *testing.T) {
	files := []string{"0001.rlp", "0002.rlp", "0003.rlp"}
	badBlock := errors.New("invalid block")
	var imported []string
	err := importFiles(files, log.Root(), func(fn string) error {
		imported = append(imported, fn)
		if fn == "0002.rlp" {
			return badBlock
		}
		return nil
	})
	require.ErrorIs(t, err, badBlock)
	require.Equal(t, files, imported, "a failing block file must not stop import of later files")
}

func TestImportFilesSingleFileSurfacesError(t *testing.T) {
	badBlock := errors.New("invalid block")
	err := importFiles([]string{"0001.rlp"}, log.Root(), func(fn string) error {
		return badBlock
	})
	require.ErrorIs(t, err, badBlock)
}

func TestImportFilesStopsOnInterrupt(t *testing.T) {
	files := []string{"0001.rlp", "0002.rlp", "0003.rlp"}
	var imported []string
	err := importFiles(files, log.Root(), func(fn string) error {
		imported = append(imported, fn)
		if fn == "0002.rlp" {
			return errInterrupted
		}
		return nil
	})
	require.ErrorIs(t, err, errInterrupted)
	require.Equal(t, []string{"0001.rlp", "0002.rlp"}, imported, "user interrupt must abort the whole import, not just skip the current file")
}

func TestChainHasBlockPropagatesViewError(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db.Close() // View must fail once the underlying DB is closed

	block := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(1)}, nil)
	has, err := ChainHasBlock(db, block)
	require.Error(t, err)
	require.False(t, has)
}

func TestMissingBlocksPropagatesViewError(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db.Close() // View must fail once the underlying DB is closed

	blocks := []*types.Block{
		types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(1)}, nil),
	}
	missing, err := missingBlocks(db, blocks)
	require.Error(t, err)
	require.Nil(t, missing)
}

func TestMissingBlocksReturnsSuffixFromFirstMissingBlock(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	blocks := make([]*types.Block, 3)
	for i := range blocks {
		blocks[i] = types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(uint64(i) + 1)}, nil)
	}

	// Only the first block is present in the DB; blocks[1:] are missing.
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdb.WriteBody(tx, blocks[0].Hash(), blocks[0].NumberU64(), &types.Body{}))
	require.NoError(t, tx.Commit())

	has, err := ChainHasBlock(db, blocks[0])
	require.NoError(t, err)
	require.True(t, has, "block written to the DB must be reported present")

	missing, err := missingBlocks(db, blocks)
	require.NoError(t, err)
	require.Equal(t, blocks[1:], missing, "missingBlocks must return the suffix starting at the first absent block")
}
