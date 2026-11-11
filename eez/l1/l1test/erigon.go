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
package l1test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// emitter codes: A logs X(calldata word), B logs Y.
var (
	emitterACode = append(append([]byte{0x60, 0x00, 0x35, 0x7f}, TopicX[:]...), 0x60, 0x00, 0x60, 0x00, 0xa2, 0x00)
	emitterBCode = append(append([]byte{0x7f}, TopicY[:]...), 0x60, 0x00, 0x60, 0x00, 0xa1, 0x00)
)

// ErigonL1 returns an Erigon L1 of 11 blocks whose last three replaced an
// earlier branch, with block 4 finalized, and the same chain as a model.
// Every block calls both emitters and stores a new root in Registry; block
// CreatedAt also creates Created.
func ErigonL1(t *testing.T) (*execmoduletester.ExecModuleTester, *recording.MemoryChain) {
	t.Helper()
	ctx := context.Background()
	key, err := crypto.HexToECDSA(sampleKey)
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	m := execmoduletester.New(t, execmoduletester.WithKey(key), execmoduletester.WithGenesisSpec(&types.Genesis{
		Config:   chain.TestChainOsakaConfig,
		GasLimit: 30_000_000,
		Alloc: types.GenesisAlloc{
			sender:   {Balance: new(big.Int).Lsh(big.NewInt(1), 96)},
			EmitterA: {Balance: new(big.Int), Code: emitterACode},
			EmitterB: {Balance: new(big.Int), Code: emitterBCode},
			Registry: {Balance: new(big.Int), Code: registryCode},
		},
	}))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	// word is the input of call j in block i of branch; the third call
	// stores it in Registry.
	word := func(branch, i, j int) common.Hash { return common.BigToHash(big.NewInt(int64(1000*branch + 10*i + j))) }
	// The fork is generated from genesis with the first seven blocks identical,
	// since block generation executes on the latest state, not the parent's.
	gen := func(forkAt int) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			branch := 0
			if i >= forkAt {
				branch = 1
			}
			nonce := uint64(3 * i)
			if i >= CreatedAt {
				nonce++
			}
			for j, to := range []common.Address{EmitterA, EmitterB, Registry} {
				data := word(branch, i, j)
				tx, err := types.SignTx(types.NewEIP1559Transaction(*m.ChainConfig.ChainID, nonce, to, new(uint256.Int), 100_000,
					uint256.NewInt(1), uint256.NewInt(100_000_000_000), data[:]), *signer, key)
				require.NoError(t, err)
				b.AddTx(tx)
				nonce++
			}
			if i+1 == CreatedAt {
				tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 100_000, uint256.NewInt(100_000_000_000), createCode), *signer, key)
				require.NoError(t, err)
				b.AddTx(tx)
			}
		}
	}
	first, err := m.GenerateChain(10, gen(10))
	require.NoError(t, err)
	forked, err := m.GenerateChain(11, gen(7))
	require.NoError(t, err)
	require.Equal(t, first.Blocks[6].Hash(), forked.Blocks[6].Hash())
	require.Equal(t, Created, first.Receipts[CreatedAt-1][3].ContractAddress)
	fork := forked.Slice(7, 11)

	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, first.Blocks))
	finalised := execmoduletester.WithFinalisedHash(first.Blocks[3].Hash())
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, fork.Blocks, execmoduletester.WithFcuOptSeq([][]execmoduletester.UFCOpt{
		{finalised}, {finalised}, {finalised}, {finalised},
	})))

	model, err := recording.NewMemoryChain(m.ChainConfig.ChainID.Uint64(), withState(modelBlock(m.Genesis, nil), common.Hash{}))
	require.NoError(t, err)
	for i := range 7 {
		require.NoError(t, model.Append(withState(modelBlock(first.Blocks[i], first.Receipts[i]), word(0, i, 2))))
	}
	require.NoError(t, model.Finalize(4))
	for i := 7; i < 10; i++ {
		require.NoError(t, model.Append(withState(modelBlock(first.Blocks[i], first.Receipts[i]), word(0, i, 2))))
	}
	branch := make([]*recording.Block, len(fork.Blocks))
	for i := range fork.Blocks {
		branch[i] = withState(modelBlock(fork.Blocks[i], fork.Receipts[i]), word(1, 7+i, 2))
	}
	require.NoError(t, model.Reorg(7, branch...))
	return m, model
}

func modelBlock(b *types.Block, receipts types.Receipts) *recording.Block {
	out := &recording.Block{
		Header: l1.Header{Number: b.NumberU64(), Hash: b.Hash(), ParentHash: b.ParentHash(), Time: b.Time(), MixDigest: b.MixDigest()},
		Txs:    b.Transactions(),
	}
	for i, r := range receipts {
		for _, l := range r.Logs {
			out.Logs = append(out.Logs, &types.Log{Address: l.Address, Topics: l.Topics, Data: l.Data, TxIndex: hexutil.Uint(i)})
		}
	}
	return out
}
