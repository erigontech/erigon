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
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMessage_AccessorsAndSetters(t *testing.T) {
	t.Parallel()
	from := accounts.InternAddress(common.HexToAddress("0x01"))
	to := accounts.InternAddress(common.HexToAddress("0x02"))
	al := AccessList{{Address: common.HexToAddress("0x03"), StorageKeys: []common.Hash{common.HexToHash("0x04")}}}

	m := NewMessage(from, to, 7, uint256.NewInt(100), 21_000,
		uint256.NewInt(1), uint256.NewInt(3), uint256.NewInt(2),
		[]byte{1, 2}, al, true, true, true, false, uint256.NewInt(5))

	require.Equal(t, from, m.From())
	require.Equal(t, to, m.To())
	require.Equal(t, uint64(7), m.Nonce())
	require.Equal(t, uint64(21_000), m.Gas())
	require.Equal(t, uint64(100), m.Value().Uint64())
	require.Equal(t, uint64(1), m.GasPrice().Uint64())
	require.Equal(t, uint64(3), m.FeeCap().Uint64())
	require.Equal(t, uint64(2), m.TipCap().Uint64())
	require.Equal(t, []byte{1, 2}, m.Data())
	require.Equal(t, al, m.AccessList())
	require.True(t, m.CheckNonce())
	require.True(t, m.CheckTransaction())
	require.True(t, m.CheckGas())
	require.False(t, m.IsFree())
	require.Equal(t, uint64(5), m.MaxFeePerBlobGas().Uint64())

	// Setters.
	m.SetIsFree(true)
	require.True(t, m.IsFree())
	m.SetCheckNonce(false)
	require.False(t, m.CheckNonce())
	m.SetCheckTransaction(false)
	require.False(t, m.CheckTransaction())
	m.SetCheckGas(false)
	require.False(t, m.CheckGas())

	m.ChangeGas(50_000, 30_000)
	require.Equal(t, uint64(30_000), m.Gas())

	hashes := []common.Hash{common.HexToHash("0xaa")}
	m.SetBlobVersionedHashes(hashes)
	require.Equal(t, hashes, m.BlobHashes())
	require.Equal(t, params.GasPerBlob, m.BlobGas())

	auths := []Authorization{{Nonce: 1}}
	m.SetAuthorizations(auths)
	require.Len(t, m.Authorizations(), 1)
}

func TestCalcEffectiveGasTip(t *testing.T) {
	t.Parallel()
	tip := uint256.NewInt(2)
	feeCap := uint256.NewInt(10)
	getTip := func() *uint256.Int { return tip }
	getFee := func() *uint256.Int { return feeCap }

	// No base fee -> tip cap.
	require.Equal(t, *tip, CalcEffectiveGasTip(nil, getTip, getFee))
	// Fee cap below base fee -> zero.
	require.Equal(t, uint256.Int{}, CalcEffectiveGasTip(uint256.NewInt(20), getTip, getFee))
	// effectiveFee (10-5=5) > tip (2) -> tip.
	require.Equal(t, *uint256.NewInt(2), CalcEffectiveGasTip(uint256.NewInt(5), getTip, getFee))
	// effectiveFee (10-9=1) < tip (2) -> effectiveFee.
	require.Equal(t, *uint256.NewInt(1), CalcEffectiveGasTip(uint256.NewInt(9), getTip, getFee))
}

func TestTransactions_LenEncodeIndex(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := LatestSignerForChainID(uint256.NewInt(1))
	to := common.HexToAddress("0x0000000000000000000000000000000000000002")
	signed, err := SignTx(NewTransaction(0, to, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), *signer, key)
	require.NoError(t, err)

	txs := Transactions{signed}
	require.Equal(t, 1, txs.Len())

	var buf bytes.Buffer
	txs.EncodeIndex(0, &buf)
	require.NotEmpty(t, buf.Bytes())
}
