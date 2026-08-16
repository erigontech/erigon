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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
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
)

// TestRecreateStorageClearedAcrossBlocks reproduces eest
// test_recreate[recreate_on_separate_block_True] (valid_until Shanghai): a
// contract is created and given storage in block 1, self-destructed in block 2,
// then re-created at the same CREATE2 address in block 3. Pre-Cancun (no
// EIP-6780) the self-destruct fully removes the account, so the re-created
// contract must observe EMPTY storage — the block-1 slot must not survive the
// cross-block destruct+recreate. Runs both serial and parallel executors.
func TestRecreateStorageClearedAcrossBlocks(t *testing.T) {
	// creator: CALLDATACOPY(0,0,CALLDATASIZE); CREATE2(0,0,CALLDATASIZE,0)
	creatorCode := common.FromHex("0x36600060003760003660006000f5")
	// deploy_code: if CALLVALUE==0 selfdestruct(0), else SSTORE(0, CALLVALUE)
	deployCode := common.FromHex("0x34600014600c5734600055005b6000ff")
	// initcode returning deploy_code
	initcode := common.FromHex("0x6010600c60003960106000f334600014600c5734600055005b6000ff")

	for _, mode := range []struct {
		name     string
		parallel bool
	}{
		{"serial", false},
		{"parallel", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			prev := dbg.Exec3Parallel
			dbg.Exec3Parallel = mode.parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prev })

			shanghaiConfig := new(chain.Config)
			require.NoError(t, copier.CopyWithOption(shanghaiConfig, chain.TestChainOsakaConfig, copier.Option{DeepCopy: true}))
			shanghaiConfig.CancunTime = nil
			shanghaiConfig.PragueTime = nil
			shanghaiConfig.OsakaTime = nil

			senderKey, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(senderKey.PublicKey)
			creatorAddr := common.HexToAddress("0x00000000000000000000000000000000c0dec0de")

			gspec := &types.Genesis{
				Config: shanghaiConfig,
				Alloc: types.GenesisAlloc{
					sender:      {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(19), nil)},
					creatorAddr: {Code: creatorCode, Balance: big.NewInt(0)},
				},
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(senderKey))

			var salt [32]byte
			created := types.CreateAddress2(creatorAddr, salt, accounts.InternCodeHash(crypto.Keccak256Hash(initcode)))

			signer := types.LatestSignerForChainID(shanghaiConfig.ChainID)
			gasPrice := uint256.NewInt(10_000_000_000)
			mkTx := func(nonce uint64, to common.Address, value uint64, data []byte) types.Transaction {
				txn, err := types.SignTx(types.NewTransaction(nonce, to, uint256.NewInt(value), 200_000, gasPrice, data), *signer, senderKey)
				require.NoError(t, err)
				return txn
			}

			chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				switch i {
				case 0: // create + set storage[0]=1
					b.AddTx(mkTx(b.TxNonce(sender), creatorAddr, 0, initcode))
					b.AddTx(mkTx(b.TxNonce(sender), created, 1, nil))
				case 1: // self-destruct + send funds (revive balance-only)
					b.AddTx(mkTx(b.TxNonce(sender), created, 0, nil))
					b.AddTx(mkTx(b.TxNonce(sender), created, 1, nil))
				case 2: // recreate at the same address
					b.AddTx(mkTx(b.TxNonce(sender), creatorAddr, 0, initcode))
				}
			})
			require.NoError(t, err)

			for blk := 0; blk < 3; blk++ {
				require.NoErrorf(t, m.InsertChain(chainPack.Slice(blk, blk+1)), "insert block %d", blk+1)
			}

			require.NoError(t, m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				addr := accounts.InternAddress(created)

				code, err := st.GetCode(addr)
				require.NoError(t, err)
				require.Equal(t, deployCode, code, "recreated contract must carry deploy_code")

				nonce, err := st.GetNonce(addr)
				require.NoError(t, err)
				require.Equal(t, uint64(1), nonce, "recreated contract nonce")

				bal, err := st.GetBalance(addr)
				require.NoError(t, err)
				require.Equal(t, uint64(1), bal.Uint64(), "recreated contract balance")

				slot0, err := st.GetState(addr, accounts.InternKey(common.Hash{}))
				require.NoError(t, err)
				require.True(t, slot0.IsZero(), "storage slot 0 must be cleared on recreate, got %s", slot0.String())
				return nil
			}))
		})
	}
}
