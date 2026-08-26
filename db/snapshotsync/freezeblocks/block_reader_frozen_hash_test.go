// Copyright 2024 The Erigon Authors
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

// TestBlockReaderFrozenHashIdentity pins the invariant that a BlockReader
// lookup naming an exact block hash must never answer with a different
// block, even once the requested height has been retired into snapshot
// segments (which are indexed by height only and hold canonical data).
//
// It builds two blocks at the same height with different hashes, makes one
// canonical, freezes and prunes the range so only the snapshot-backed path
// can answer, then asks every hash-accepting BlockReader method for the
// orphaned block's exact hash.
func TestBlockReaderFrozenHashIdentity(t *testing.T) {
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

	// The orphan branch is short; the canonical branch must clear one full
	// segment so height 1 is covered by frozen snapshot files.
	orphanBranch := makeBranch(5, common.HexToAddress("0xAA"), 111)
	canonicalBranch := makeBranch(int(snaptype.Erigon2MinSegmentSize)+10, common.HexToAddress("0xBB"), 222)

	require.NoError(t, m.InsertChain(orphanBranch))
	require.NoError(t, m.InsertChain(canonicalBranch)) // reorgs the orphan branch out

	orphanBlock := orphanBranch.Blocks[0]
	canonicalBlock := canonicalBranch.Blocks[0]
	require.Equal(t, uint64(1), orphanBlock.NumberU64())
	require.Equal(t, orphanBlock.NumberU64(), canonicalBlock.NumberU64())
	require.NotEqual(t, orphanBlock.Hash(), canonicalBlock.Hash())

	// Baseline: before freezing, the orphan is still a genuine, servable block
	// through the ordinary DB path — so a later nil result is the freeze/prune
	// transition losing its identity, not a fixture that never worked.
	preFreezeTx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer preFreezeTx.Rollback()
	preFreezeHeader, err := m.BlockReader.Header(m.Ctx, preFreezeTx, orphanBlock.Hash(), orphanBlock.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, preFreezeHeader, "baseline: the orphan must be servable by its own hash before freezing")
	require.Equal(t, orphanBlock.Hash(), preFreezeHeader.Hash())
	preFreezeTx.Rollback()

	logger := log.New()
	snCfg, _ := snapcfg.KnownCfg(networkname.Mainnet)
	snCfg.ExpectBlocks = math.MaxUint64
	require.NoError(t, freezeblocks.DumpBlocks(m.Ctx, 0, snaptype.Erigon2MinSegmentSize, m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap,
		m.DB, 1, log.LvlInfo, logger, m.BlockReader, snCfg, nil))
	require.NoError(t, m.BlockSnapshots.OpenFolder())
	require.GreaterOrEqual(t, m.BlockSnapshots.BlocksAvailable(), orphanBlock.NumberU64())

	rwTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	_, err = rawdb.PruneBlocks(rwTx, snaptype.Erigon2MinSegmentSize, 2*int(snaptype.Erigon2MinSegmentSize))
	require.NoError(t, err)
	require.NoError(t, rwTx.Commit())

	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Control: the DB row for the orphan header is gone, and HeaderByHash does a
	// genuine hash-keyed snapshot search rather than a positional one, so it must
	// not find (and must not substitute) anything for the orphan's hash.
	ctrlHeader, err := m.BlockReader.HeaderByHash(m.Ctx, tx, orphanBlock.Hash())
	require.NoError(t, err)
	require.Nil(t, ctrlHeader, "control: HeaderByHash must not substitute the canonical sibling")

	t.Run("orphan hash at frozen height is rejected, not substituted", func(t *testing.T) {
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotHeader, "Header must not return the canonical sibling for a mismatched hash")

		gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBlock, "BlockWithSenders must not return the canonical sibling for a mismatched hash")

		gotBodyWithTxs, err := m.BlockReader.BodyWithTransactions(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBodyWithTxs, "BodyWithTransactions must not return the canonical sibling's body for a mismatched hash")

		gotBody, _, err := m.BlockReader.Body(m.Ctx, tx, orphanBlock.Hash(), orphanBlock.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBody, "Body must not return the canonical sibling's body for a mismatched hash")
	})

	t.Run("canonical hash at frozen height still resolves", func(t *testing.T) {
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
		require.Equal(t, canonicalBlock.Transactions()[0].Hash(), gotBodyWithTxs.Transactions[0].Hash(),
			"the returned body must be the canonical block's own body, not merely non-nil")

		gotBody, gotTxCount, err := m.BlockReader.Body(m.Ctx, tx, canonicalBlock.Hash(), canonicalBlock.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotBody)
		require.Equal(t, uint32(1), gotTxCount)
	})

	t.Run("zero hash keeps existing height-only behavior", func(t *testing.T) {
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
	})
}

// TestBlockReaderFrozenHashIdentityWrongHeight adversarially checks a case the
// primary regression test does not cover: a hash that is genuinely canonical,
// but requested at the WRONG height. A fix that only compared "is this hash
// canonical somewhere" without pinning it to the specific requested height
// would wrongly accept this; a correct fix must reject it exactly like a
// non-canonical hash.
func TestBlockReaderFrozenHashIdentityWrongHeight(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}

	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	segSize := int(snaptype.Erigon2MinSegmentSize)
	chainPack, err := m.GenerateChain(segSize+10, func(i int, b *blockgen.BlockGen) {
		to := common.HexToAddress("0xCC")
		tx, txErr := types.SignTx(
			&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    b.TxNonce(m.Address),
					To:       &to,
					GasLimit: params.TxGas,
					Value:    *uint256.NewInt(uint64(i + 1)),
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
	require.NoError(t, m.InsertChain(chainPack))

	blockAtHeight1 := chainPack.Blocks[0]
	blockAtHeight2 := chainPack.Blocks[1]
	require.Equal(t, uint64(1), blockAtHeight1.NumberU64())
	require.Equal(t, uint64(2), blockAtHeight2.NumberU64())
	require.NotEqual(t, blockAtHeight1.Hash(), blockAtHeight2.Hash())

	logger := log.New()
	snCfg, _ := snapcfg.KnownCfg(networkname.Mainnet)
	snCfg.ExpectBlocks = math.MaxUint64
	require.NoError(t, freezeblocks.DumpBlocks(m.Ctx, 0, uint64(segSize), m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap,
		m.DB, 1, log.LvlInfo, logger, m.BlockReader, snCfg, nil))
	require.NoError(t, m.BlockSnapshots.OpenFolder())
	require.GreaterOrEqual(t, m.BlockSnapshots.BlocksAvailable(), blockAtHeight2.NumberU64())

	rwTx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	_, err = rawdb.PruneBlocks(rwTx, uint64(segSize), 2*segSize)
	require.NoError(t, err)
	require.NoError(t, rwTx.Commit())

	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	t.Run("genuinely canonical hash requested at the wrong height is rejected", func(t *testing.T) {
		// blockAtHeight1.Hash() is a real, canonical hash — just not the one that
		// belongs at height 2.
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, blockAtHeight1.Hash(), blockAtHeight2.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotHeader, "Header must not accept a canonical hash from a different height")

		gotBlock, _, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, blockAtHeight1.Hash(), blockAtHeight2.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBlock, "BlockWithSenders must not accept a canonical hash from a different height")

		gotBodyWithTxs, err := m.BlockReader.BodyWithTransactions(m.Ctx, tx, blockAtHeight1.Hash(), blockAtHeight2.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBodyWithTxs, "BodyWithTransactions must not accept a canonical hash from a different height")

		gotBody, _, err := m.BlockReader.Body(m.Ctx, tx, blockAtHeight1.Hash(), blockAtHeight2.NumberU64())
		require.NoError(t, err)
		require.Nil(t, gotBody, "Body must not accept a canonical hash from a different height")
	})

	t.Run("right hash at right height still resolves near this boundary", func(t *testing.T) {
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, blockAtHeight2.Hash(), blockAtHeight2.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, gotHeader)
		require.Equal(t, blockAtHeight2.Hash(), gotHeader.Hash())
	})

	t.Run("boundary heights: last frozen vs first non-frozen", func(t *testing.T) {
		lastFrozen := m.BlockSnapshots.BlocksAvailable()
		require.Greater(t, lastFrozen, uint64(0))
		firstNonFrozen := lastFrozen + 1
		require.LessOrEqual(t, firstNonFrozen, uint64(chainPack.Length()))

		lastFrozenBlock := chainPack.Blocks[lastFrozen-1]
		firstNonFrozenBlock := chainPack.Blocks[firstNonFrozen-1]

		// Right hash, right height, at the very last frozen height: must resolve.
		gotHeader, err := m.BlockReader.Header(m.Ctx, tx, lastFrozenBlock.Hash(), lastFrozen)
		require.NoError(t, err)
		require.NotNil(t, gotHeader)
		require.Equal(t, lastFrozenBlock.Hash(), gotHeader.Hash())

		// Right hash, right height, at the first non-frozen (still DB-backed) height: must resolve.
		gotHeader2, err := m.BlockReader.Header(m.Ctx, tx, firstNonFrozenBlock.Hash(), firstNonFrozen)
		require.NoError(t, err)
		require.NotNil(t, gotHeader2)
		require.Equal(t, firstNonFrozenBlock.Hash(), gotHeader2.Hash())

		// Cross-boundary wrong-height request must still be rejected on both sides.
		gotHeader3, err := m.BlockReader.Header(m.Ctx, tx, lastFrozenBlock.Hash(), firstNonFrozen)
		require.NoError(t, err)
		require.Nil(t, gotHeader3, "a frozen height's hash must not be accepted for the adjacent non-frozen height")
	})
}
