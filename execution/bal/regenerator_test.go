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

package bal_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestRegeneratorReproducesCanonicalBlockAccessLists(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	storingInitCode := []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x00} // PUSH1 1, PUSH1 0, SSTORE, STOP
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, b *blockgen.BlockGen) {
		switch i {
		case 0:
			txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(10_000), 50_000, baseFee, nil), *signer, privKey)
			require.NoError(t, err)
			b.AddTx(txn)
		case 1:
			txn, err := types.SignTx(types.NewContractCreation(1, uint256.NewInt(0), 500_000, baseFee, storingInitCode), *signer, privKey)
			require.NoError(t, err)
			b.AddTx(txn)
		case 2:
			txn, err := types.SignTx(types.NewTransaction(2, common.Address{2}, uint256.NewInt(20_000), 50_000, baseFee, nil), *signer, privKey)
			require.NoError(t, err)
			b.AddTx(txn)
			txn2, err := types.SignTx(types.NewTransaction(3, common.Address{3}, uint256.NewInt(30_000), 50_000, baseFee, nil), *signer, privKey)
			require.NoError(t, err)
			b.AddTx(txn2)
		case 3:
			// empty block — its BAL is the canonical empty list
		}
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, 4)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)
	pruneStoredBALs(t, m)
	gen := bal.NewRegenerator(m.BlockReader, m.Engine, m.Log)
	ttx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer ttx.Rollback()
	for i, block := range chainPack.Blocks {
		expected := chainPack.BlockAccessLists[i]
		require.NotEmpty(t, expected, "block %d should have a canonical BAL", block.NumberU64())
		got, err := gen.GetBlockAccessListBytes(ctx, m.ChainConfig, ttx, block.Hash(), block.NumberU64())
		require.NoError(t, err, "block %d", block.NumberU64())
		require.Equal(t, expected, got, "block %d", block.NumberU64())
		decoded, err := types.DecodeBlockAccessListBytes(got)
		require.NoError(t, err)
		header := block.Header()
		require.NotNil(t, header.BlockAccessListHash)
		require.Equal(t, *header.BlockAccessListHash, decoded.Hash())
	}
}

func TestRegeneratorReturnsNilForPreAmsterdamBlocks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	cfg := &chain.Config{
		ChainID:                       uint256.NewInt(1337),
		Rules:                         chain.EtHashRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		DepositContract:               common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa"),
		Ethash:                        new(chain.EthashConfig),
	}
	genesis := &types.Genesis{
		Config: cfg,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(10_000), 50_000, baseFee, nil), *signer, privKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)
	require.Nil(t, chainPack.Blocks[0].Header().BlockAccessListHash)
	gen := bal.NewRegenerator(m.BlockReader, m.Engine, m.Log)
	ttx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer ttx.Rollback()
	got, err := gen.GetBlockAccessListBytes(ctx, m.ChainConfig, ttx, chainPack.Blocks[0].Hash(), chainPack.Blocks[0].NumberU64())
	require.NoError(t, err)
	require.Nil(t, got)
}

// pruneStoredBALs drops every stored BAL row, simulating the exec-stage prune
// that keeps BALs only for the reorg window.
func pruneStoredBALs(t *testing.T, m *execmoduletester.ExecModuleTester) {
	t.Helper()
	err := m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.ForEach(kv.BlockAccessList, nil, func(k, _ []byte) error {
			return tx.Delete(kv.BlockAccessList, k)
		})
	})
	require.NoError(t, err)
	err = m.DB.View(t.Context(), func(tx kv.Tx) error {
		count, err := tx.Count(kv.BlockAccessList)
		require.NoError(t, err)
		require.Zero(t, count, "stored BALs should be fully pruned")
		return nil
	})
	require.NoError(t, err)
}
