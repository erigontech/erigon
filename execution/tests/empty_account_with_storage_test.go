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
	"fmt"
	"math/big"
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
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/ethconfig"
)

func storageAt(t *testing.T, st *state.IntraBlockState, addr common.Address, slot byte) uint256.Int {
	t.Helper()
	v, err := st.GetState(accounts.InternAddress(addr), accounts.InternKey(common.BytesToHash([]byte{slot})))
	require.NoError(t, err)
	return v
}

// A tx touches an empty account that holds storage, so EIP-161 removes the
// account with its storage. A later tx delegates the account by EIP-7702 to
// code that stores SLOAD(1)+1 in slot 2, so slot 2 must be 1.
func TestTouchedEmptyAccountWithStorageIsWiped(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })
	ethconfig.Defaults.Sync.ExecWorkerCount = 4

	authorityKey, err := crypto.ToECDSA(common.LeftPadBytes([]byte{0xa0}, 32))
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	delegate := common.HexToAddress("0xd0d0")
	burner := common.HexToAddress("0xbbbb")
	_, loop := program.New().Jumpdest()
	burnerCode := program.New().Op(vm.JUMPDEST).Push(loop).Op(vm.JUMP).Bytes()
	gasPrice := uint256.NewInt(10_000_000_000)

	for _, fork := range []struct {
		name      string
		amsterdam bool
	}{{"prague", false}, {"amsterdam", true}} {
		t.Run(fork.name, func(t *testing.T) {
			config := chain.TestChainOsakaConfig.Copy()
			if fork.amsterdam {
				config.AmsterdamTime = common.NewUint64(0)
			} else {
				config.OsakaTime = nil
			}
			alloc := types.GenesisAlloc{
				burner:    {Code: burnerCode},
				delegate:  {Code: program.New().Push(1).Op(vm.SLOAD).Push(1).Op(vm.ADD).Push(2).Op(vm.SSTORE, vm.STOP).Bytes()},
				authority: {Balance: big.NewInt(0), Storage: map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
			}
			keys := revivalKeys(t, alloc, 3)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
			signer := types.LatestSignerForChainID(config.ChainID)
			auth, err := types.SignAuthorization(authorityKey, *config.ChainID, delegate, 0)
			require.NoError(t, err)

			generate := func(versioned bool) *blockgen.ChainPack {
				dbg.Exec3Parallel = versioned
				m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
				chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
					b.SetCoinbase(common.Address{1})
					for i, to := range []common.Address{burner, authority} {
						gas := uint64(100_000)
						if to == burner {
							gas = 1_000_000
						}
						sender := crypto.PubkeyToAddress(keys[i].PublicKey)
						txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), to, uint256.NewInt(0), gas, gasPrice, nil), *signer, keys[i])
						require.NoError(t, err)
						b.AddTx(txn)
					}
					sender := crypto.PubkeyToAddress(keys[2].PublicKey)
					setCode, err := types.SignTx(&types.SetCodeTransaction{
						DynamicFeeTransaction: types.DynamicFeeTransaction{
							CommonTx: types.CommonTx{Nonce: b.TxNonce(sender), GasLimit: 500_000, To: &authority},
							ChainID:  *config.ChainID,
							TipCap:   *gasPrice,
							FeeCap:   *gasPrice,
						},
						Authorizations: []types.Authorization{auth},
					}, *signer, keys[2])
					require.NoError(t, err)
					b.AddTx(setCode)
				})
				require.NoError(t, err)
				return chainPack
			}

			chainPack := generate(true)
			t.Run("serial generation", func(t *testing.T) {
				require.Equal(t, chainPack.TopBlock.Root(), generate(false).TopBlock.Root())
			})
			for _, parallel := range []bool{false, true} {
				t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
					dbg.Exec3Parallel = parallel
					m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
					require.NoError(t, m.InsertChain(chainPack))
					require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
						st := state.New(m.NewStateReader(tx))
						defer st.Close()
						require.Equal(t, uint256.Int{}, storageAt(t, st, authority, 1))
						require.Equal(t, *uint256.NewInt(1), storageAt(t, st, authority, 2))
						return nil
					}))
				})
			}
		})
	}
}
