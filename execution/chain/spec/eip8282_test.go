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

package chainspec_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/misc"
)

func TestDeveloperGenesisBuilderContracts(t *testing.T) {
	genesis := chainspec.DeveloperGenesisBlock()
	tests := []struct {
		name    string
		address common.Address
		code    []byte
	}{
		{
			name:    "deposit",
			address: common.HexToAddress("0x0000BFF46984E3725691FA540A8C7589300D8282"),
			code:    misc.BuilderDepositRequestCode,
		},
		{
			name:    "exit",
			address: common.HexToAddress("0x000064D678505AD48F8CCB093BC65613800E8282"),
			code:    misc.BuilderExitRequestCode,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				account, ok := genesis.Alloc[tt.address]
				require.True(t, ok)
				require.Equal(t, uint64(1), account.Nonce)
				require.Equal(t, tt.code, account.Code)
			},
		)
	}
}
