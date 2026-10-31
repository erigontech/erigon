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

package executiontests

import (
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// A chain config with a burnt-fee contract credits the base fee to that
// contract. The contract may also be the coinbase, a sender or a callee of the
// block; parallel import must accept the block that serial execution built and
// end with the same balances.
func TestBurntContractFeeRecipients(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })

	alloc := types.GenesisAlloc{}
	keys := revivalKeys(t, alloc, 2)
	sender := crypto.PubkeyToAddress(keys[0].PublicKey)
	coinbase := common.HexToAddress("0xc0ffee")
	callee := common.HexToAddress("0xca11ee")
	alloc[callee] = types.GenesisAccount{Code: []byte{byte(vm.STOP)}}
	fresh := common.HexToAddress("0xb0b0")
	watched := []common.Address{sender, crypto.PubkeyToAddress(keys[1].PublicKey), coinbase, callee, fresh}

	for _, tc := range []struct {
		name  string
		burnt common.Address
	}{{"fresh account", fresh}, {"coinbase", coinbase}, {"sender", sender}, {"callee", callee}} {
		t.Run(tc.name, func(t *testing.T) {
			config := chain.TestChainOsakaConfig.Copy()
			config.BurntContract = map[string]common.Address{"0": tc.burnt}
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
			signer := types.LatestSignerForChainID(config.ChainID)
			transfer := func(key *ecdsa.PrivateKey, value, tip uint64) types.Transaction {
				txn, err := types.SignTx(&types.DynamicFeeTransaction{
					CommonTx: types.CommonTx{Nonce: 0, GasLimit: 100_000, To: &callee, Value: *uint256.NewInt(value)},
					ChainID:  *config.ChainID,
					TipCap:   *uint256.NewInt(tip),
					FeeCap:   *uint256.NewInt(10_000_000_000),
				}, *signer, key)
				require.NoError(t, err)
				return txn
			}

			dbg.Exec3Parallel = false
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
			chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				b.SetCoinbase(coinbase)
				b.AddTx(transfer(keys[0], 0, 3))
				b.AddTx(transfer(keys[1], 2_000, 5))
			})
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(chainPack))
			want := balancesOf(t, m, watched)

			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				for i, addr := range watched {
					bal, err := st.GetBalance(accounts.InternAddress(addr))
					require.NoError(t, err)
					require.Equal(t, want[i], bal, "balance of %x", addr)
				}
			})
		})
	}
}

func balancesOf(t *testing.T, m *execmoduletester.ExecModuleTester, addrs []common.Address) []uint256.Int {
	t.Helper()
	out := make([]uint256.Int, len(addrs))
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		for i, addr := range addrs {
			bal, err := st.GetBalance(accounts.InternAddress(addr))
			if err != nil {
				return err
			}
			out[i] = bal
		}
		return nil
	}))
	return out
}
