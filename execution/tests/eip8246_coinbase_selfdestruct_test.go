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

// TestEIP8246CoinbaseSelfdestructPreservesBalance pins a parallel-executor
// balance-burn regression under EIP-8246 (Amsterdam).
//
// Scenario: the block's COINBASE is a contract that is created and, in the same
// tx, SELFDESTRUCTs to itself while holding a non-zero balance B. Under EIP-8246
// SELFDESTRUCT no longer burns, so B is preserved on the (code/nonce-less)
// coinbase — leaving a balance-only account that the version map records as
// self-destructed. A second, ordinary fee-paying tx makes the executor's
// calcFees credit the block's tips onto that coinbase.
//
// The parallel executor's calcFees reads the coinbase through a
// versionedStateReader with an empty ReadSet. The fork-agnostic
// VersionMap.IsNetAbsent verdict reports the self-destructed account as absent,
// so without the EIP-8246-aware reconstruction of the preserved account from the
// version-map cells, calcFees credits the tips onto 0 and the preserved balance
// B is burned from the state root — a parallel-only divergence from the serial
// executor (which reconstructs the preserved account directly). InsertChain
// re-executes and checks the state root against the header, so the burn surfaces
// as a root mismatch there too.
//
// The exact same block is run through both the serial and the parallel executor;
// the coinbase must end with B + accumulated tips (strictly greater than B) under
// both, proving B survived rather than being replaced by tips alone.
func TestEIP8246CoinbaseSelfdestructPreservesBalance(t *testing.T) {
	// childInitcode = ADDRESS SELFDESTRUCT: the deployed child self-destructs to
	// itself during construction, so under EIP-8246 its whole endowment is
	// preserved on the (now code/nonce-less) account.
	childInitcode := common.FromHex("0x30ff")
	childCodeHash := accounts.InternCodeHash(crypto.Keccak256Hash(childInitcode))

	// factoryCode endows a CREATE2 child with B and deploys childInitcode:
	//   PUSH2 0x30ff PUSH1 0 MSTORE          ; mem[30:32] = childInitcode
	//   PUSH1 0 (salt) PUSH1 2 (size) PUSH1 30 (offset) PUSH7 B (value) CREATE2 STOP
	// B = 7000000000000000 wei = 0x18de76816d8000.
	factoryCode := common.FromHex("0x6130ff60005260006002601e6618de76816d8000f500")
	preservedBalance := uint256.NewInt(7_000_000_000_000_000) // B

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

			amsterdamConfig := new(chain.Config)
			require.NoError(t, copier.CopyWithOption(amsterdamConfig, chain.TestChainOsakaConfig, copier.Option{DeepCopy: true}))
			amsterdamConfig.AmsterdamTime = common.NewUint64(0)

			senderKey, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(senderKey.PublicKey)
			factory := common.HexToAddress("0x00000000000000000000000000000000fac70247")
			recipient := common.HexToAddress("0x00000000000000000000000000000000000ec1b7")

			// The block's coinbase is the CREATE2 child the factory deploys — a
			// deterministic address independent of the sender key.
			var salt [32]byte
			coinbase := types.CreateAddress2(factory, salt, childCodeHash)

			gspec := &types.Genesis{
				Config: amsterdamConfig,
				Alloc: types.GenesisAlloc{
					sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(19), nil)},
					// Fund the factory so its CREATE2 can endow the child with B.
					factory: {Code: factoryCode, Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
				},
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(senderKey))

			signer := types.LatestSignerForChainID(amsterdamConfig.ChainID)
			mkTx := func(nonce uint64, to common.Address, value uint64, gas uint64, gasPrice *uint256.Int) types.Transaction {
				txn, err := types.SignTx(types.NewTransaction(nonce, to, uint256.NewInt(value), gas, gasPrice, nil), *signer, senderKey)
				require.NoError(t, err)
				return txn
			}

			tipGasPrice := uint256.NewInt(10_000_000_000)
			chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
				b.SetCoinbase(coinbase)
				// tx0 self-destructs the coinbase with a zero priority tip
				// (gasPrice == baseFee), so calcFees emits no coinbase write for it.
				// That leaves the coinbase net-absent (self-destructed, no revival)
				// when tx1's positive-tip calcFees reads it — the exact state where
				// the fork-agnostic IsNetAbsent verdict would burn the preserved B.
				baseFee := b.GetHeader().BaseFee

				// tx0: call the factory, which CREATE2-deploys the coinbase child
				// endowed with B; the child self-destructs to itself, so EIP-8246
				// preserves B on the coinbase.
				b.AddTx(mkTx(b.TxNonce(sender), factory, 0, 500_000, baseFee))
				// tx1: an ordinary fee-paying transfer with a positive priority tip,
				// so tips accrue to the (now net-absent) coinbase via calcFees.
				b.AddTx(mkTx(b.TxNonce(sender), recipient, 1, 21_000, tipGasPrice))
			})
			require.NoError(t, err)

			require.NoError(t, m.InsertChain(chainPack))

			require.NoError(t, m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()

				bal, err := st.GetBalance(accounts.InternAddress(coinbase))
				require.NoError(t, err)
				// If B were burned, the coinbase would hold only the tips (< B). B
				// surviving plus the tips must leave it strictly above B.
				require.Truef(t, bal.Cmp(preservedBalance) > 0,
					"coinbase balance %s must exceed preserved B %s: EIP-8246 preserved balance was burned", bal, preservedBalance)
				return nil
			}))
		})
	}
}
