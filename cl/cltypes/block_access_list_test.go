// Copyright 2024 The Erigon Authors
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

package cltypes

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestBlockAccessListRoundtrip(t *testing.T) {
	bal := NewBlockAccessList(1024)
	require.NoError(t, bal.SetBytes([]byte{0x01, 0x02, 0x03, 0x04}))

	encoded, err := bal.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded := NewBlockAccessList(1024)
	require.NoError(t, decoded.DecodeSSZ(encoded, 0))
	require.Equal(t, bal.Bytes(), decoded.Bytes())

	root1, err := bal.HashSSZ()
	require.NoError(t, err)
	root2, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root1, root2)
}

func TestEth1PayloadHeaderIncludesBlockAccessListRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	payload := NewEth1Block(clparams.GloasVersion, &cfg)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.BlockHash[0] = 1 // force transactions root path

	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xaa, 0xbb, 0xcc}))
	expectedRoot, err := payload.BlockAccessList.HashSSZ()
	require.NoError(t, err)

	header, err := payload.PayloadHeader()
	require.NoError(t, err)
	require.Equal(t, header.BlockAccessListRoot[:], expectedRoot[:])
}

func TestNewEth1BlockFromHeaderAndBodyWithBlockAccessListBytes(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	balHash := common.HexToHash("0x01")
	header := &types.Header{
		BaseFee:             big.NewInt(1),
		Number:              big.NewInt(1),
		BlockAccessListHash: &balHash,
	}
	body := &types.RawBody{}
	balBytes := []byte{0xc0}

	payload, err := NewEth1BlockFromHeaderAndBody(header, body, &cfg, balBytes)
	require.NoError(t, err)
	require.NotNil(t, payload.BlockAccessList)
	require.Equal(t, balBytes, payload.BlockAccessList.Bytes())
}

func TestNewEth1BlockFromHeaderAndBodyRejectsPreGloasWithBlockAccessListBytes(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	header := &types.Header{
		BaseFee: big.NewInt(1),
		Number:  big.NewInt(1),
	}
	body := &types.RawBody{}

	_, err := NewEth1BlockFromHeaderAndBody(header, body, &cfg, []byte{0xc0})
	require.Error(t, err)
}
