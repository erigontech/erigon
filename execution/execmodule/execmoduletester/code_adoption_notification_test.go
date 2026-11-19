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

package execmoduletester

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestCodeAdoptionOnNewHeadAndDuringCatchup(t *testing.T) {
	defer func(prev bool) { dbg.BALDrivenCommitment = prev }(dbg.BALDrivenCommitment)
	defer func(prev bool) { dbg.IgnoreBAL = prev }(dbg.IgnoreBAL)
	dbg.BALDrivenCommitment, dbg.IgnoreBAL = true, false

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	caller := crypto.PubkeyToAddress(key.PublicKey)
	template := common.Address{0xbb}
	adopted := []byte{byte(vm.STOP)}
	alloc := types.GenesisAlloc{
		caller:   {Balance: big.NewInt(common.Ether)},
		template: {Code: adopted, Nonce: 1},
	}
	adopters := []common.Address{{0xa1}, {0xa2}, {0xa3}, {0xa4}, {0xa5}}
	for _, addr := range adopters {
		alloc[addr] = types.GenesisAccount{Code: program.New().Push(template).Op(vm.SETCODEFROM).Bytes(), Nonce: 1}
	}
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(0)
	m := New(t, WithKey(key), WithGenesisSpec(&types.Genesis{Config: config, Alloc: alloc}),
		WithExperimentalBAL(), WithAlwaysGenerateChangesets(false), WithMaxReorgDepth(2))
	signer := *types.LatestSignerForChainID(config.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
	pack, err := m.GenerateChain(len(adopters), func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(uint64(i), adopters[i], uint256.NewInt(0), 200_000, gasPrice, nil), signer, key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	ctx := t.Context()
	waitForBatch := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, pack.Blocks[:1]))
	m.ExecModule.WaitIdle(ctx)
	batch := waitForBatch(pack.Blocks[0].Hash())
	var announced *remoteproto.AccountChange
	for _, change := range batch.ChangeBatch {
		for _, account := range change.Changes {
			if gointerfaces.ConvertH160toAddress(account.Address) == adopters[0] {
				announced = account
			}
		}
	}
	require.NotNil(t, announced)
	require.Equal(t, remoteproto.Action_UPSERT_CODE, announced.Action)
	require.Equal(t, adopted, announced.Code)
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		data, _, err := tx.GetLatest(kv.AccountsDomain, adopters[0][:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Equal(t, data, announced.Data)
		return nil
	}))

	stagedsync.ResetComputedAheadForTest()
	status, err := m.InsertBlocks(ctx, pack.Blocks[1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(ctx, pack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, result.ValidationError)
	m.ExecModule.WaitIdle(ctx)
	require.Positive(t, stagedsync.ComputedAheadCountForTest())
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		for _, addr := range adopters {
			code, _, err := tx.GetLatest(kv.CodeDomain, addr[:], kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Equal(t, adopted, code)
		}
		return nil
	}))
}
