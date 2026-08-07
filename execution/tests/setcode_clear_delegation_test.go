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
	"bytes"
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
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSetCodeClearDelegationPurgesCodeDomain verifies the account/code invariant
// across the EIP-7702 delegation lifecycle: setting a delegation stores its
// designator in CodeDomain, while clearing it deletes the entry and restores
// the authority's empty code hash.
func TestSetCodeClearDelegationPurgesCodeDomain(t *testing.T) {
	// This test changes dbg.Exec3Parallel and cannot run in parallel.
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

			senderKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			authorityKey, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(senderKey.PublicKey)
			authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
			delegate := common.HexToAddress("0x000000000000000000000000000000000000cafe")

			pragueConfig := new(chain.Config)
			require.NoError(t, copier.CopyWithOption(pragueConfig, chain.TestChainOsakaConfig, copier.Option{DeepCopy: true}))
			pragueConfig.OsakaTime = nil
			setAuth, err := types.SignAuthorization(authorityKey, *pragueConfig.ChainID, delegate, 0)
			require.NoError(t, err)
			clearAuth, err := types.SignAuthorization(authorityKey, *pragueConfig.ChainID, common.Address{}, 1)
			require.NoError(t, err)
			gspec := &types.Genesis{
				Config: pragueConfig,
				Alloc: types.GenesisAlloc{
					sender: {Balance: big.NewInt(1_000_000_000_000_000_000)},
				},
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(senderKey))

			signer := types.LatestSignerForChainID(pragueConfig.ChainID)
			mkSetCodeTx := func(nonce uint64, auth types.Authorization) types.Transaction {
				to := common.HexToAddress("0x000000000000000000000000000000000000beef")
				txn := &types.SetCodeTransaction{
					DynamicFeeTransaction: types.DynamicFeeTransaction{
						CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 500_000, To: &to},
						ChainID:  *pragueConfig.ChainID,
						TipCap:   *uint256.NewInt(1_000_000_000),
						FeeCap:   *uint256.NewInt(10_000_000_000),
					},
					Authorizations: []types.Authorization{auth},
				}
				signed, err := types.SignTx(txn, *signer, senderKey)
				require.NoError(t, err)
				return signed
			}

			chainPack, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				switch i {
				case 0:
					b.AddTx(mkSetCodeTx(b.TxNonce(sender), setAuth))
				case 1:
					b.AddTx(mkSetCodeTx(b.TxNonce(sender), clearAuth))
				}
			})
			require.NoError(t, err)

			readAuthority := func() (accounts.Account, []byte) {
				var acc accounts.Account
				var code []byte
				require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
					accEnc, _, err := tx.GetLatest(kv.AccountsDomain, authority[:])
					if err != nil {
						return err
					}
					if len(accEnc) > 0 {
						if err := accounts.DeserialiseV3(&acc, accEnc); err != nil {
							return err
						}
					}
					codeVal, _, err := tx.GetLatest(kv.CodeDomain, authority[:])
					if err != nil {
						return err
					}
					// GetLatest can hand back tx-owned bytes, and the tx is closed
					// before the assertions run.
					code = bytes.Clone(codeVal)
					return nil
				}))
				return acc, code
			}

			require.NoError(t, m.InsertChain(chainPack.Slice(0, 1)))
			acc, code := readAuthority()
			require.Equal(t, uint64(1), acc.Nonce, "set authorization must have been applied")
			require.Equal(t, types.AddressToDelegation(accounts.InternAddress(delegate)), code)
			require.False(t, acc.IsEmptyCodeHash())

			require.NoError(t, m.InsertChain(chainPack.Slice(1, 2)))
			acc, code = readAuthority()
			require.Equal(t, uint64(2), acc.Nonce, "clear authorization must have been applied")
			require.True(t, acc.IsEmptyCodeHash(), "account code hash must be empty after delegation clear")
			require.Empty(t, code, "CodeDomain must not retain the delegation designator after clear")
		})
	}
}
