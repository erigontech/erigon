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

package v3

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestAccountCodec(t *testing.T) {
	codeHash, storageRoot := common.HexToHash("0x1234"), common.HexToHash("0x5678")
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E1-E2/account-values", func(t *testing.T) {
			for _, tt := range []struct {
				name string
				spec commitmenttest.AccountSpec
				root common.Hash
			}{
				{"eoa", commitmenttest.AccountSpec{CodeHash: empty.CodeHash}, empty.RootHash},
				{"contract with storage", commitmenttest.AccountSpec{Nonce: 3, Balance: 99, CodeHash: codeHash}, storageRoot},
				{"contract with code and no storage", commitmenttest.AccountSpec{Nonce: 7, CodeHash: codeHash}, empty.RootHash},
				{"elision/zero/empty-code", commitmenttest.AccountSpec{CodeHash: empty.CodeHash}, empty.RootHash},
				{"elision/zero/code", commitmenttest.AccountSpec{CodeHash: codeHash}, empty.RootHash},
				{"elision/balance/empty-code", commitmenttest.AccountSpec{Balance: 99, CodeHash: empty.CodeHash}, empty.RootHash},
				{"elision/balance/code", commitmenttest.AccountSpec{Balance: 99, CodeHash: codeHash}, empty.RootHash},
			} {
				t.Run(tt.name, func(t *testing.T) {
					update := testAccountUpdate(commitmenttest.Account(tt.spec))
					update.Flags = 0
					encoded := encodeAccountLeaf(update, tt.root[:], nil)
					nonce, balance, gotCode, gotRoot, err := decodeAccountLeaf(encoded)
					require.NoError(t, err)
					require.Equal(t, tt.spec.Nonce, nonce)
					require.Equal(t, 0, balance.Cmp(uint256.NewInt(tt.spec.Balance)))
					require.Equal(t, tt.spec.Balance, balance.Uint64())
					require.Equal(t, tt.spec.CodeHash[:], gotCode)
					require.Equal(t, tt.root[:], gotRoot)
				})
			}
		}},
		{"E4/consensus", func(t *testing.T) {
			for _, balance := range []uint64{0, 99} {
				for _, code := range []common.Hash{empty.CodeHash, codeHash} {
					acc := accounts.NewAccount()
					acc.Nonce = 7
					acc.Balance.SetUint64(balance)
					acc.Root = storageRoot
					acc.CodeHash = accounts.InternCodeHash(code)
					require.Equal(t, acc.RLP(), accountConsensusRLP(acc.Nonce, &acc.Balance, acc.Root[:], code[:], nil))
				}
			}
			acc := accounts.NewAccount()
			got := accountConsensusRLP(0, &acc.Balance, empty.RootHash[:], empty.CodeHash[:], nil)
			acc.Root = empty.RootHash
			require.Equal(t, acc.RLP(), got)
		}},
		{"E2/defaults", func(t *testing.T) {
			for _, tt := range []struct {
				name   string
				update commitment.Update
				root   []byte
				want   []byte
			}{
				{"zero code", commitment.Update{}, empty.RootHash[:], []byte{0}},
				{"empty code", commitment.Update{CodeHash: empty.CodeHash}, empty.RootHash[:], []byte{0}},
				{"zero hashes", commitment.Update{CodeHash: common.Hash{}}, make([]byte, length.Hash), []byte{0}},
				{"nonce only", commitment.Update{Nonce: 1, CodeHash: empty.CodeHash}, empty.RootHash[:], []byte{accountHasNonce, 1}},
				{"balance only", commitment.Update{Balance: *uint256.NewInt(99), CodeHash: empty.CodeHash}, empty.RootHash[:], []byte{0, 99}},
			} {
				t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, encodeAccountLeaf(&tt.update, tt.root, nil)) })
			}
			full := encodeAccountLeaf(&commitment.Update{Nonce: 1, Balance: *uint256.NewInt(99), CodeHash: codeHash}, empty.RootHash[:], nil)
			require.Len(t, full, 1+1+length.Hash+1)
		}},
		{"E3/malformed", func(t *testing.T) {
			encoded := append([]byte{accountHasNonce | accountHasCodeHash, 1}, append(make([]byte, 30), 0x12, 0x34)...)
			for i := range encoded {
				_, _, _, _, err := decodeAccountLeaf(encoded[:i])
				require.Error(t, err, "truncation at %d", i)
			}
			for _, tt := range []struct {
				name string
				data []byte
			}{
				{"unknown flags", []byte{0x80, 0}},
				{"zero code", append([]byte{accountHasCodeHash, 0}, make([]byte, length.Hash)...)},
				{"zero nonce", []byte{accountHasNonce, 0}},
				{"oversized balance", append([]byte{0}, make([]byte, length.Hash+1)...)},
				{"leading zero balance", []byte{0, 0x00, 0x01}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					_, _, _, _, err := decodeAccountLeaf(tt.data)
					require.ErrorIs(t, err, errAccountLeafFlags)
				})
			}
		}},
		{"E1-E101/seeded-decode", func(t *testing.T) {
			for _, share := range []int{12, 0} {
				t.Run(fmt.Sprintf("contracts=%d", share), func(t *testing.T) {
					rnd := rand.New(rand.NewSource(7))
					for i := range 200000 {
						value, root := commitmenttest.SizedAccount(i, i%100 < share, rnd)
						encoded := encodeAccountLeaf(testAccountUpdate(value), root, nil)
						nonce, balance, code, storage, err := decodeAccountLeaf(encoded)
						require.NoError(t, err, "account=%d", i)
						require.Equal(t, value.Nonce, nonce)
						require.Equal(t, value.Balance, balance)
						require.Equal(t, value.CodeHash[:], code)
						require.Equal(t, root, storage)
						_ = accountConsensusRLP(nonce, &balance, storage, code, nil)
					}
					if share == 0 {
						return
					}
					for range 200000 {
						n := 1 + rnd.Intn(32)
						if rnd.Intn(100) < 55 {
							n = 1 + rnd.Intn(4)
						}
						value := make([]byte, n)
						rnd.Read(value)
						value[0] |= 0x01
						require.Equal(t, storageLeafRefBuffered(nil, value, nil), storageLeafRef(nil, value, nil))
					}
				})
			}
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}
