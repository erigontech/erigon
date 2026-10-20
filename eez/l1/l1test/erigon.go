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
// Every block calls both emitters.
func ErigonL1(t *testing.T) (*execmoduletester.ExecModuleTester, *recording.MemoryChain) {
	t.Helper()
	ctx := context.Background()
	key, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	m := execmoduletester.New(t, execmoduletester.WithKey(key), execmoduletester.WithGenesisSpec(&types.Genesis{
		Config:   chain.TestChainOsakaConfig,
		GasLimit: 30_000_000,
		Alloc: types.GenesisAlloc{
			sender:   {Balance: new(big.Int).Lsh(big.NewInt(1), 96)},
			EmitterA: {Balance: new(big.Int), Code: emitterACode},
			EmitterB: {Balance: new(big.Int), Code: emitterBCode},
		},
	}))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	// The fork is generated from genesis with the first seven blocks identical,
	// since block generation executes on the latest state, not the parent's.
	gen := func(forkAt int) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			branch := 0
			if i >= forkAt {
				branch = 1
			}
			for j, to := range []common.Address{EmitterA, EmitterB} {
				data := common.BigToHash(big.NewInt(int64(1000*branch + 10*i + j)))
				tx, err := types.SignTx(types.NewEIP1559Transaction(*m.ChainConfig.ChainID, uint64(2*i+j), to, new(uint256.Int), 100_000,
					uint256.NewInt(1), uint256.NewInt(100_000_000_000), data[:]), *signer, key)
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
	fork := forked.Slice(7, 11)

	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, first.Blocks))
	finalised := execmoduletester.WithFinalisedHash(first.Blocks[3].Hash())
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, fork.Blocks, execmoduletester.WithFcuOptSeq([][]execmoduletester.UFCOpt{
		{finalised}, {finalised}, {finalised}, {finalised},
	})))

	model, err := recording.NewMemoryChain(m.ChainConfig.ChainID.Uint64(), modelBlock(m.Genesis, nil))
	require.NoError(t, err)
	for i := range 7 {
		require.NoError(t, model.Append(modelBlock(first.Blocks[i], first.Receipts[i])))
	}
	require.NoError(t, model.Finalize(4))
	for i := 7; i < 10; i++ {
		require.NoError(t, model.Append(modelBlock(first.Blocks[i], first.Receipts[i])))
	}
	branch := make([]*recording.Block, len(fork.Blocks))
	for i := range fork.Blocks {
		branch[i] = modelBlock(fork.Blocks[i], fork.Receipts[i])
	}
	require.NoError(t, model.Reorg(7, branch...))
	return m, model
}

func modelBlock(b *types.Block, receipts types.Receipts) *recording.Block {
	out := &recording.Block{
		Header: l1.Header{Number: b.NumberU64(), Hash: b.Hash(), ParentHash: b.ParentHash(), Time: b.Time()},
		Txs:    b.Transactions(),
	}
	for i, r := range receipts {
		for _, l := range r.Logs {
			out.Logs = append(out.Logs, &types.Log{Address: l.Address, Topics: l.Topics, Data: l.Data, TxIndex: hexutil.Uint(i)})
		}
	}
	return out
}
