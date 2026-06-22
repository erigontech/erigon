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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestBuildExecutionPayload_BlockAccessListGloasOnly(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig

	t.Run("pre-Gloas payload omits blockAccessList", func(t *testing.T) {
		block := cltypes.NewEth1Block(clparams.ElectraVersion, &beaconCfg)
		block.Extra = solid.NewExtraData()
		block.Transactions = &solid.TransactionsSSZ{}
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)

		ep := buildExecutionPayload(block)

		raw, err := json.Marshal(ep)
		require.NoError(t, err)

		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(raw, &m))
		_, ok := m["blockAccessList"]
		require.False(t, ok, "blockAccessList must be absent from JSON for pre-Gloas blocks")
	})

	gloasTests := []struct {
		name    string
		block   *cltypes.Eth1Block
		wantHex string
	}{
		{
			name:    "Gloas block with nil ByteListSSZ",
			block:   gloas(&beaconCfg, nil),
			wantHex: "0x",
		},
		{
			name:    "Gloas block with empty ByteListSSZ data",
			block:   gloas(&beaconCfg, []byte{}),
			wantHex: "0x",
		},
		{
			name:    "Gloas block with RLP empty list",
			block:   gloas(&beaconCfg, []byte{0xc0}),
			wantHex: "0xc0",
		},
		{
			name:    "Gloas block with non-empty BAL",
			block:   gloas(&beaconCfg, []byte{0xc1, 0x80}),
			wantHex: "0xc180",
		},
	}

	for _, tt := range gloasTests {
		t.Run(tt.name, func(t *testing.T) {
			ep := buildExecutionPayload(tt.block)

			raw, err := json.Marshal(ep)
			require.NoError(t, err)

			var m map[string]interface{}
			require.NoError(t, json.Unmarshal(raw, &m))

			bal, ok := m["blockAccessList"]
			require.True(t, ok, "blockAccessList must be present in JSON for Gloas+ blocks")
			require.Equal(t, tt.wantHex, bal)
		})
	}
}

func gloas(cfg *clparams.BeaconChainConfig, balData []byte) *cltypes.Eth1Block {
	block := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	block.Extra = solid.NewExtraData()
	block.Transactions = &solid.TransactionsSSZ{}
	block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	if balData != nil {
		bal := solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
		_ = bal.SetBytes(balData)
		block.BlockAccessList = bal
	} else {
		block.BlockAccessList = nil
	}
	return block
}
