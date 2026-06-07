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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMakeSigner_PerFork(t *testing.T) {
	t.Parallel()
	require.False(t, MakeSigner(nil, 0, 0).protected) // nil config -> empty signer

	prague := MakeSigner(chain.AllProtocolChanges, 0, 0)
	require.True(t, prague.protected)
	require.True(t, prague.dynamicFee)
	require.True(t, prague.blob)
	require.True(t, prague.setCode)

	frontier := MakeFrontierSigner()
	require.True(t, frontier.malleable)
	require.True(t, frontier.unprotected)

	latest := LatestSigner(chain.AllProtocolChanges)
	require.True(t, latest.dynamicFee)
}

func TestSigner_StringEqualMalleable(t *testing.T) {
	t.Parallel()
	s := *LatestSignerForChainID(big.NewInt(1))
	require.Contains(t, s.String(), "Signer[chainId=")
	require.True(t, s.Equal(s)) //nolint:gocritic // intentional self-comparison
	require.False(t, s.Equal(*LatestSignerForChainID(big.NewInt(2))))

	sp := LatestSignerForChainID(big.NewInt(1))
	sp.SetMalleable(true)
	require.True(t, sp.malleable)
	require.False(t, sp.Equal(s))
}

func TestSignTx_RecoverSender_Legacy(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	signer := LatestSignerForChainID(big.NewInt(1))

	to := common.HexToAddress("0x0000000000000000000000000000000000000002")
	signed, err := SignTx(NewTransaction(0, to, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), *signer, key)
	require.NoError(t, err)

	got, err := signed.Sender(*signer)
	require.NoError(t, err)
	require.Equal(t, addr, got.Value())

	// Sender is now cached.
	gs, ok := signed.GetSender()
	require.True(t, ok)
	require.Equal(t, addr, gs.Value())

	msg, err := signed.AsMessage(*signer, nil, &chain.Rules{})
	require.NoError(t, err)
	require.Equal(t, addr, msg.From().Value())
}

func TestSignTx_RecoverSender_DynamicFee(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	signer := LatestSignerForChainID(big.NewInt(1))

	to := common.HexToAddress("0x0000000000000000000000000000000000000002")
	tx := NewEIP1559Transaction(*uint256.NewInt(1), 0, to, uint256.NewInt(1), 21_000,
		uint256.NewInt(1), uint256.NewInt(1), uint256.NewInt(2), nil)
	signed, err := SignTx(tx, *signer, key)
	require.NoError(t, err)

	got, err := signed.Sender(*signer)
	require.NoError(t, err)
	require.Equal(t, addr, got.Value())

	msg, err := signed.AsMessage(*signer, uint256.NewInt(1), &chain.Rules{IsLondon: true})
	require.NoError(t, err)
	require.Equal(t, addr, msg.From().Value())
}

func TestSignTx_RecoverSender_SetCode(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	signer := LatestSignerForChainID(big.NewInt(1))

	signed, err := SignTx(sampleSetCodeTx(), *signer, key)
	require.NoError(t, err)

	got, err := signed.Sender(*signer)
	require.NoError(t, err)
	require.Equal(t, addr, got.Value())

	msg, err := signed.AsMessage(*signer, uint256.NewInt(1), &chain.Rules{IsPrague: true})
	require.NoError(t, err)
	require.Equal(t, addr, msg.From().Value())
}

func TestMustSignNewTx_And_SetSender(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := LatestSignerForChainID(big.NewInt(1))
	to := common.HexToAddress("0x0000000000000000000000000000000000000002")

	var signed Transaction
	require.NotPanics(t, func() {
		signed = MustSignNewTx(key, *signer, NewTransaction(0, to, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil))
	})
	require.NotNil(t, signed)

	// SetSender overrides the cached sender.
	override := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000000009"))
	signed.SetSender(override)
	gs, ok := signed.GetSender()
	require.True(t, ok)
	require.Equal(t, override, gs)
}
