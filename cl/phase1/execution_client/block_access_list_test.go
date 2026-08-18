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
)

func TestDecodeBlockAccessList(t *testing.T) {
	t.Parallel()
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xc0}))

	bal, err := DecodeBlockAccessList(payload)
	require.NoError(t, err)
	require.NotNil(t, bal)
	require.Empty(t, bal)

	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xc2, 0x01, 0x02}))
	_, err = DecodeBlockAccessList(payload)
	require.Error(t, err)
}
