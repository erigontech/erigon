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

package execmodule_test

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	eth1utils "github.com/erigontech/erigon/execution/execmodule/moduleutil"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
)

func TestValidateChainWithLastTxNumOfBlockAtStepBoundary(t *testing.T) {
	// See https://github.com/erigontech/erigon/issues/18823
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubKey := privKey.PublicKey
	senderAddr := crypto.PubkeyToAddress(pubKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	stepSize := uint64(5) // 2 for block 0 (0,1) and 3 for block 1 (2,3,4)
	m := mock.MockWithGenesis(t, genesis, privKey, false, mock.WithStepSize(stepSize))
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(0, senderAddr, uint256.NewInt(0), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, 1)
	newBlock := eth1utils.ConvertBlockToRPC(chainPack.Blocks[0])
	exec := m.Eth1ExecutionService
	insertRes, err := exec.InsertBlocks(ctx, &executionproto.InsertBlocksRequest{
		Blocks: []*executionproto.Block{newBlock},
	})
	require.NoError(t, err)
	require.Equal(t, executionproto.ExecutionStatus_Success, insertRes.Result)
	validationReceipt, err := exec.ValidateChain(ctx, &executionproto.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(chainPack.Blocks[0].Hash()),
		Number: chainPack.Blocks[0].NumberU64(),
	})
	require.NoError(t, err)
	require.Equal(t, executionproto.ExecutionStatus_Success, validationReceipt.ValidationStatus)
	require.Equal(t, "", validationReceipt.ValidationError)
	extendingHash, extendingNum, extendingSd := m.ForkValidator.ExtendingFork()
	require.Equal(t, chainPack.Blocks[0].Hash(), extendingHash)
	require.Equal(t, uint64(1), extendingNum)
	var inMemBlockNum, inMemTxNum uint64
	err = m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		v, _, err := extendingSd.GetLatest(kv.CommitmentDomain, tx, commitmentdb.KeyCommitmentState)
		require.NoError(t, err)
		inMemTxNum = binary.BigEndian.Uint64(v[:8])
		inMemBlockNum = binary.BigEndian.Uint64(v[8:16])
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), inMemBlockNum)
	// previously the bug was that we stored the commitment state key as if it was at block 1 txNum 3
	// when it should have been at txNum 4
	require.Equal(t, uint64(4), inMemTxNum)
	root, err := extendingSd.GetCommitmentCtx().Trie().RootHash()
	require.NoError(t, err)
	require.Equal(t, chainPack.Headers[0].Root, common.BytesToHash(root))
}
