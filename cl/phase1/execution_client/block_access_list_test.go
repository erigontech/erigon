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

package execution_client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestDecodeAndValidateBlockAccessList(t *testing.T) {
	t.Parallel()
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xc0}))

	bal, err := DecodeAndValidateBlockAccessList(payload)
	require.NoError(t, err)
	require.NotNil(t, bal)
	require.Empty(t, bal.BlockAccessList())
	raw, err := bal.Bytes()
	require.NoError(t, err)
	require.Equal(t, []byte{0xc0}, raw)

	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xc2, 0x01, 0x02}))
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.Error(t, err)

	oversized, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
	}})
	require.NoError(t, err)
	require.NoError(t, payload.BlockAccessList.SetBytes(oversized))
	payload.GasLimit = types.BalItemCost - 1
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.ErrorContains(t, err, "block access list too large")
}
