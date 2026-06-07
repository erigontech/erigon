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

package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
)

func sampleAccessListTx() *AccessListTx {
	to := common.HexToAddress("0x0000000000000000000000000000000000000002")
	return &AccessListTx{
		LegacyTx: LegacyTx{
			CommonTx: CommonTx{Nonce: 0, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(1)},
			GasPrice: *uint256.NewInt(1),
		},
		ChainID: *uint256.NewInt(1),
		AccessList: AccessList{{
			Address:     common.HexToAddress("0x0000000000000000000000000000000000000003"),
			StorageKeys: []common.Hash{common.HexToHash("0x04"), common.HexToHash("0x05")},
		}},
	}
}

func TestAccessList_StorageKeys(t *testing.T) {
	t.Parallel()
	al := AccessList{
		{StorageKeys: []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")}},
		{StorageKeys: []common.Hash{common.HexToHash("0x03")}},
	}
	require.Equal(t, 3, al.StorageKeys())
	require.Equal(t, 0, AccessList{}.StorageKeys())
}

func TestAccessListTx_Basics(t *testing.T) {
	t.Parallel()
	tx := sampleAccessListTx()
	require.True(t, tx.Protected())
	require.Nil(t, tx.GetAuthorizations())
	require.Same(t, tx, tx.Unwrap())
	require.Equal(t, 2, tx.GetAccessList().StorageKeys())
}

func TestAccessListTx_SignRecoverAsMessage(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	signer := LatestSignerForChainID(uint256.NewInt(1).ToBig())

	signed, err := SignTx(sampleAccessListTx(), *signer, key)
	require.NoError(t, err)

	got, err := signed.Sender(*signer)
	require.NoError(t, err)
	require.Equal(t, addr, got.Value())

	cs, ok := signed.GetSender()
	require.True(t, ok)
	require.Equal(t, addr, cs.Value())

	alTx, ok := signed.(*AccessListTx)
	require.True(t, ok)
	csa, ok := alTx.cachedSender()
	require.True(t, ok)
	require.Equal(t, addr, csa.Value())

	msg, err := signed.AsMessage(*signer, nil, &chain.Rules{IsBerlin: true})
	require.NoError(t, err)
	require.Equal(t, addr, msg.From().Value())
}
