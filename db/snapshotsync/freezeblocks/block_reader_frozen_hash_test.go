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

package freezeblocks_test

import (
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// TestBlockReaderFrozenHashLookup covers BlockReader lookups that name an exact
// block hash at a height the block files already cover. Segments are indexed by
// height and hold canonical blocks only, so a positional read there answers for
// one block; anything else must come from the db or not at all.
//
// Both same-height (reorged-out sibling) and cross-height (real hash, wrong
// number) mismatches are driven against a height that is frozen *and* pruned, so
// the files are the only thing left that could answer.
func TestBlockReaderFrozenHashLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}

	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	makeBranch := func(length int, to common.Address, value uint64) *blockgen.ChainPack {
		branch, err := m.GenerateChain(length, func(i int, b *blockgen.BlockGen) {
			if i > 1 {
				return
			}
			tx, txErr := types.SignTx(
				&types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    b.TxNonce(m.Address),
						To:       &to,
						GasLimit: params.TxGas,
						Value:    *uint256.NewInt(value + uint64(i)),
					},
					GasPrice: *uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2),
				},
				*types.LatestSignerForChainID(m.ChainConfig.ChainID),
				m.Key,
			)
			require.NoError(t, txErr)
			b.AddTx(tx)
		})
		require.NoError(t, err)
		return branch
	}

	// The orphan branch is short; the canonical branch must clear one full
	// segment so the fork height ends up covered by the block files.
	segSize := int(snaptype.Erigon2MinSegmentSize)
	orphanBranch := makeBranch(5, common.HexToAddress("0xAA"), 111)
	canonicalBranch := makeBranch(segSize+10, common.HexToAddress("0xBB"), 222)

	require.NoError(t, m.InsertChain(orphanBranch))
	require.NoError(t, m.InsertChain(canonicalBranch)) // reorgs the orphan branch out

	orphanBlock := orphanBranch.Blocks[0]
	canonicalBlock := canonicalBranch.Blocks[0]
	nextCanonicalBlock := canonicalBranch.Blocks[1]
	require.Equal(t, uint64(1), orphanBlock.NumberU64())
	require.Equal(t, orphanBlock.NumberU64(), canonicalBlock.NumberU64())
	require.NotEqual(t, orphanBlock.Hash(), canonicalBlock.Hash())
	require.Equal(t, uint64(2), nextCanonicalBlock.NumberU64())

	// The orphan is a real, servable block before freezing, so a nil answer
	// later is the freeze/prune transition and not a fixture that never worked.
	preFreezeTx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer preFreezeTx.Rollback()
	preFreezeHeader, err := m.BlockReader.Header(m.Ctx, preFreezeTx, orphanBlock.Hash(), orphanBlock.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, preFreezeHeader)
	require.Equal(t, orphanBlock.Hash(), preFreezeHeader.Hash())
	preFreezeTx.Rollback()

	logger := log.New()
	// KnownCfg hands back a process-wide cached pointer, so copy before tuning.
	knownCfg, _ := snapcfg.KnownCfg(networkname.Mainnet)
	snCfg := *knownCfg
	snCfg.ExpectBlocks = math.MaxUint64
	require.NoError(t, freezeblocks.DumpBlocks(m.Ctx, 0, uint64(segSize), m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap,
		m.DB, 1, log.LvlInfo, logger, m.BlockReader, &snCfg, nil))
	require.NoError(t, m.BlockSnapshots.OpenFolder())

	frozenTip := m.BlockSnapshots.BlocksAvailable()
	require.GreaterOrEqual(t, frozenTip, nextCanonicalBlock.NumberU64())

	// Prune past the segment end so every frozen height is genuinely db-less -
	// stopping at segSize would leave the last frozen height answerable from the
	// db and the mismatch branch would never be reached.
	rwTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	_, err = rawdb.PruneBlocks(rwTx, uint64(segSize)+1, 2*segSize)
	require.NoError(t, err)
	require.NoError(t, rwTx.Commit())

	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// The orphan's db rows are gone, and HeaderByHash searches the files by hash
	// rather than by position, so nothing outside the positional path can answer
	// for this hash.
	require.Nil(t, rawdb.ReadHeader(tx, orphanBlock.Hash(), orphanBlock.NumberU64()))
	ctrlHeader, err := m.BlockReader.HeaderByHash(m.Ctx, tx, orphanBlock.Hash())
	require.NoError(t, err)
	require.Nil(t, ctrlHeader)

	t.Run("reorged-out sibling at a frozen height is not answered", func(t *testing.T) {
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotHeader, "Header returned the canonical sibling")

		gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBlock, "BlockWithSenders returned the canonical sibling")

		gotBodyWithTxs, err := m.BlockReader.BodyWithTransactions(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBodyWithTxs, "BodyWithTransactions returned the canonical sibling's body")

		gotBody, _, err := m.BlockReader.Body(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBody, "Body returned the canonical sibling's body")

		// Must be nil rather than empty: a nil body encodes to an empty list, and
		// the HasBlock shims read availability off BodyRlp being non-nil.
		gotBodyRlp, err := m.BlockReader.BodyRlp(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBodyRlp, "BodyRlp encoded a nil body as an empty block body")

		hasSenders, err := m.BlockReader.HasSenders(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.False(t, hasSenders, "HasSenders claimed senders for a block the reader will not return")
	})

	t.Run("canonical hash at the wrong frozen height is not answered", func(t *testing.T) {
		// A real canonical hash, asked for at the height of its neighbour.
		wrongHeight := nextCanonicalBlock.NumberU64()

		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, canonicalBlock.Hash(), wrongHeight)
		require.NoError(t, err)
		require.Nil(t, gotHeader)

		gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, canonicalBlock.Hash(), wrongHeight)
		require.NoError(t, err)
		require.Nil(t, gotBlock)

		gotBodyWithTxs, err := m.BlockReader.BodyWithTransactions(m.Ctx, tx, canonicalBlock.Hash(), wrongHeight)
		require.NoError(t, err)
		require.Nil(t, gotBodyWithTxs)

		gotBody, _, err := m.BlockReader.Body(m.Ctx, tx, canonicalBlock.Hash(), wrongHeight)
		require.NoError(t, err)
		require.Nil(t, gotBody)
	})

	t.Run("canonical hash at a frozen height still resolves", func(t *testing.T) {
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, canonicalBlock.Hash(), canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotHeader)
		require.Equal(t, canonicalBlock.Hash(), gotHeader.Hash())

		gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, canonicalBlock.Hash(), canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotBlock)
		require.Equal(t, canonicalBlock.Hash(), gotBlock.Hash())

		gotBodyWithTxs, err := m.BlockReader.BodyWithTransactions(m.Ctx, tx, canonicalBlock.Hash(), canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotBodyWithTxs)
		require.Len(t, gotBodyWithTxs.Transactions, 1)
		require.Equal(t, canonicalBlock.Transactions()[0].Hash(), gotBodyWithTxs.Transactions[0].Hash())

		gotBody, gotTxCount, err := m.BlockReader.Body(m.Ctx, tx, canonicalBlock.Hash(), canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotBody)
		require.Equal(t, uint32(1), gotTxCount)

		gotBodyRlp, err := m.BlockReader.BodyRlp(m.Ctx, tx, canonicalBlock.Hash(), canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotEmpty(t, gotBodyRlp)
	})

	t.Run("zero hash reads by height only", func(t *testing.T) {
		// BlockByNumber leaves the hash empty for frozen heights, so this path
		// carries no hash constraint and must keep resolving by height.
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, common.Hash{}, canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotHeader)
		require.Equal(t, canonicalBlock.Hash(), gotHeader.Hash())

		gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, common.Hash{}, canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotBlock)
		require.Equal(t, canonicalBlock.Hash(), gotBlock.Hash())

		gotBody, _, err := m.BlockReader.Body(m.Ctx, tx, common.Hash{}, canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotBody)

		byNumber, err := m.BlockReader.BlockByNumber(m.Ctx, tx, canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, byNumber)
		require.Equal(t, canonicalBlock.Hash(), byNumber.Hash())
	})
}

// TestBlockReaderFrozenHashFallsBackToDB pins the window between retiring and
// pruning: retire runs at tip-MaxReorgDepth while CanDeleteTo holds 1024 blocks
// back, so a reorged-out sibling can sit at a height the files already cover and
// still be fully readable from the db. It must be served from there rather than
// refused because the files disagree.
func TestBlockReaderFrozenHashFallsBackToDB(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}

	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	makeBranch := func(length int, to common.Address, value uint64) *blockgen.ChainPack {
		branch, err := m.GenerateChain(length, func(i int, b *blockgen.BlockGen) {
			if i != 0 {
				return
			}
			tx, txErr := types.SignTx(
				&types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    b.TxNonce(m.Address),
						To:       &to,
						GasLimit: params.TxGas,
						Value:    *uint256.NewInt(value),
					},
					GasPrice: *uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2),
				},
				*types.LatestSignerForChainID(m.ChainConfig.ChainID),
				m.Key,
			)
			require.NoError(t, txErr)
			b.AddTx(tx)
		})
		require.NoError(t, err)
		return branch
	}

	segSize := int(snaptype.Erigon2MinSegmentSize)
	orphanBranch := makeBranch(5, common.HexToAddress("0xAA"), 111)
	canonicalBranch := makeBranch(segSize+10, common.HexToAddress("0xBB"), 222)

	require.NoError(t, m.InsertChain(orphanBranch))
	require.NoError(t, m.InsertChain(canonicalBranch))

	orphanBlock := orphanBranch.Blocks[0]
	canonicalBlock := canonicalBranch.Blocks[0]
	require.NotEqual(t, orphanBlock.Hash(), canonicalBlock.Hash())

	logger := log.New()
	knownCfg, _ := snapcfg.KnownCfg(networkname.Mainnet)
	snCfg := *knownCfg
	snCfg.ExpectBlocks = math.MaxUint64
	require.NoError(t, freezeblocks.DumpBlocks(m.Ctx, 0, uint64(segSize), m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap,
		m.DB, 1, log.LvlInfo, logger, m.BlockReader, &snCfg, nil))
	require.NoError(t, m.BlockSnapshots.OpenFolder())
	require.GreaterOrEqual(t, m.BlockSnapshots.BlocksAvailable(), orphanBlock.NumberU64())

	// Deliberately not pruned: this is the frozen-but-unpruned window.
	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NotNil(t, rawdb.ReadHeader(tx, orphanBlock.Hash(), orphanBlock.NumberU64()),
		"fixture: the orphan must still be in the db for this window to exist")

	gotHeader, err := m.BlockReader.Header(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, gotHeader)
	require.Equal(t, orphanBlock.Hash(), gotHeader.Hash())

	gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, gotBlock, "BlockWithSenders refused a block that is still in the db")
	require.Equal(t, orphanBlock.Hash(), gotBlock.Hash())

	gotBodyWithTxs, err := m.BlockReader.BodyWithTransactions(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, gotBodyWithTxs, "BodyWithTransactions refused a body that is still in the db")
	require.Equal(t, orphanBlock.Transactions()[0].Hash(), gotBodyWithTxs.Transactions[0].Hash())

	gotBody, _, err := m.BlockReader.Body(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, gotBody, "Body refused a body that is still in the db")
}
