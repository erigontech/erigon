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
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func signAuthorization(t *testing.T, key *ecdsa.PrivateKey, chainID uint256.Int, delegate common.Address, nonce uint64) types.Authorization {
	t.Helper()
	auth := types.Authorization{ChainID: chainID, Address: delegate, Nonce: nonce}
	payloadLen := rlp.Uint256Len(auth.ChainID) + 1 + length.Addr + rlp.U64Len(auth.Nonce)
	var buf [32]byte
	data := bytes.NewBuffer(nil)
	require.NoError(t, rlp.EncodeStructSizePrefix(payloadLen, data, buf[:]))
	require.NoError(t, rlp.EncodeUint256(auth.ChainID, data, buf[:]))
	require.NoError(t, rlp.EncodeOptionalAddress(&auth.Address, data, buf[:]))
	require.NoError(t, rlp.EncodeInt(auth.Nonce, data, buf[:]))
	sighash := crypto.Keccak256Hash(append([]byte{params.SetCodeMagicPrefix}, data.Bytes()...))
	sig, err := crypto.Sign(sighash[:], key)
	require.NoError(t, err)
	auth.R.SetBytes(sig[:32])
	auth.S.SetBytes(sig[32:64])
	auth.YParity = sig[64]
	recovered, err := auth.RecoverSigner(bytes.NewBuffer(nil), buf[:])
	require.NoError(t, err)
	require.Equal(t, crypto.PubkeyToAddress(key.PublicKey), *recovered)
	return auth
}

// TestSetCodeClearDelegationPurgesCodeDomain pins the account↔code domain
// invariant across the EIP-7702 delegation lifecycle: setting a delegation
// stores the designator in CodeDomain; clearing it (authorization to the zero
// address) must delete that CodeDomain entry rather than leave the stale
// designator alongside an account whose code hash says "no code".
func TestSetCodeClearDelegationPurgesCodeDomain(t *testing.T) {
	// mutates dbg.Exec3Parallel per sub-test; not safe for t.Parallel
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

			cfg := &chain.Config{
				ChainID:                       big.NewInt(1337),
				HomesteadBlock:                big.NewInt(0),
				TangerineWhistleBlock:         big.NewInt(0),
				SpuriousDragonBlock:           big.NewInt(0),
				ByzantiumBlock:                big.NewInt(0),
				ConstantinopleBlock:           big.NewInt(0),
				PetersburgBlock:               big.NewInt(0),
				IstanbulBlock:                 big.NewInt(0),
				MuirGlacierBlock:              big.NewInt(0),
				BerlinBlock:                   big.NewInt(0),
				LondonBlock:                   big.NewInt(0),
				ArrowGlacierBlock:             big.NewInt(0),
				GrayGlacierBlock:              big.NewInt(0),
				TerminalTotalDifficulty:       big.NewInt(0),
				TerminalTotalDifficultyPassed: true,
				ShanghaiTime:                  big.NewInt(0),
				CancunTime:                    big.NewInt(0),
				PragueTime:                    big.NewInt(0),
				Ethash:                        new(chain.EthashConfig),
			}
			gspec := &types.Genesis{
				Config: cfg,
				Alloc: types.GenesisAlloc{
					sender: {Balance: big.NewInt(1_000_000_000_000_000_000)},
				},
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(senderKey))

			signer := types.LatestSignerForChainID(cfg.ChainID)
			mkSetCodeTx := func(nonce uint64, auth types.Authorization) types.Transaction {
				to := common.HexToAddress("0x000000000000000000000000000000000000beef")
				txn := &types.SetCodeTransaction{
					DynamicFeeTransaction: types.DynamicFeeTransaction{
						CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 500_000, To: &to},
						ChainID:  uint256.NewInt(1337),
						TipCap:   uint256.NewInt(1_000_000_000),
						FeeCap:   uint256.NewInt(10_000_000_000),
					},
					Authorizations: []types.Authorization{auth},
				}
				signed, err := types.SignTx(txn, *signer, senderKey)
				require.NoError(t, err)
				return signed
			}

			chainID := *uint256.NewInt(1337)
			chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				switch i {
				case 0:
					b.AddTx(mkSetCodeTx(b.TxNonce(sender), signAuthorization(t, authorityKey, chainID, delegate, 0)))
				case 1:
					b.AddTx(mkSetCodeTx(b.TxNonce(sender), signAuthorization(t, authorityKey, chainID, common.Address{}, 1)))
				}
			})
			require.NoError(t, err)

			readAuthority := func() (accounts.Account, []byte) {
				var acc accounts.Account
				var code []byte
				require.NoError(t, m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
					accEnc, _, err := tx.GetLatest(kv.AccountsDomain, authority[:])
					if err != nil {
						return err
					}
					if len(accEnc) > 0 {
						if err := accounts.DeserialiseV3(&acc, accEnc); err != nil {
							return err
						}
					}
					code, _, err = tx.GetLatest(kv.CodeDomain, authority[:])
					return err
				}))
				return acc, code
			}

			require.NoError(t, m.InsertChain(chainPack.Slice(0, 1)))
			acc, code := readAuthority()
			require.EqualValues(t, 1, acc.Nonce, "set authorization must have been applied")
			require.Equal(t, types.AddressToDelegation(accounts.InternAddress(delegate)), code)
			require.False(t, acc.IsEmptyCodeHash())

			require.NoError(t, m.InsertChain(chainPack.Slice(1, 2)))
			acc, code = readAuthority()
			require.EqualValues(t, 2, acc.Nonce, "clear authorization must have been applied")
			require.True(t, acc.IsEmptyCodeHash(), "account code hash must be empty after delegation clear")
			require.Empty(t, code, "CodeDomain must not retain the delegation designator after clear")
		})
	}
}
