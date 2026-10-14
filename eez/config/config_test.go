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
package config

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const devnetL2 = `{"name":"eez","rollupId":1,"l1ChainId":7331,"eezRegistry":"0x5fbdb2315678afecb367f032d93f642f64180aa3","registryDeployBlock":12,"blockInterval":2,"l1BlockTime":12,"blockGasLimit":30000000}`

func TestFromJSON(t *testing.T) {
	c, err := FromJSON([]byte(devnetL2))
	require.NoError(t, err)
	require.Equal(t, &L2Config{
		RollupID:            1,
		L1ChainID:           7331,
		Registry:            common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3"),
		RegistryDeployBlock: 12,
		BlockInterval:       2,
		L1BlockTime:         12,
		BlockGasLimit:       30_000_000,
	}, c)
	require.Equal(t, Name, c.Name())
	require.Equal(t, uint64(6), c.BlocksPerL1Block())
}

func TestJSONRoundTrip(t *testing.T) {
	c, err := FromJSON([]byte(devnetL2))
	require.NoError(t, err)
	enc, err := json.Marshal(c)
	require.NoError(t, err)
	require.JSONEq(t, devnetL2, string(enc))
	back, err := FromJSON(enc)
	require.NoError(t, err)
	require.Equal(t, c, back)
}

func TestFromJSONRejects(t *testing.T) {
	for _, tc := range []struct {
		name, json string
	}{
		{"not an object", `[]`},
		{"trailing data", devnetL2 + `{}`},
		{"unknown field", `{"name":"eez","rollupId":1,"l1ChainId":7331,"eezRegistry":"0x5fbdb2315678afecb367f032d93f642f64180aa3","blockInterval":2,"l1BlockTime":12,"blockGasLimit":30000000,"extra":0}`},
		{"missing name", `{"rollupId":1,"l1ChainId":7331,"eezRegistry":"0x5fbdb2315678afecb367f032d93f642f64180aa3","blockInterval":2,"l1BlockTime":12,"blockGasLimit":30000000}`},
		{"other stack", `{"name":"op","rollupId":1,"l1ChainId":7331,"eezRegistry":"0x5fbdb2315678afecb367f032d93f642f64180aa3","blockInterval":2,"l1BlockTime":12,"blockGasLimit":30000000}`},
		{"invalid", `{"name":"eez","rollupId":1,"l1ChainId":7331,"eezRegistry":"0x5fbdb2315678afecb367f032d93f642f64180aa3","blockInterval":2,"l1BlockTime":3,"blockGasLimit":30000000}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FromJSON([]byte(tc.json))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestValidate(t *testing.T) {
	valid := func() L2Config {
		return L2Config{
			RollupID:      1,
			L1ChainID:     10200,
			Registry:      common.HexToAddress("0x01"),
			BlockInterval: 1,
			L1BlockTime:   5,
			BlockGasLimit: 30_000_000,
		}
	}
	base := valid()
	require.NoError(t, base.Validate())

	for _, tc := range []struct {
		name  string
		mod   func(*L2Config)
		valid bool
	}{
		{"zero registry deploy block", func(c *L2Config) { c.RegistryDeployBlock = 0 }, true},
		{"two blocks per L1 block", func(c *L2Config) { c.BlockInterval, c.L1BlockTime = 2, 4 }, true},
		{"zero rollup id", func(c *L2Config) { c.RollupID = 0 }, false},
		{"zero L1 chain id", func(c *L2Config) { c.L1ChainID = 0 }, false},
		{"zero registry", func(c *L2Config) { c.Registry = common.Address{} }, false},
		{"zero block interval", func(c *L2Config) { c.BlockInterval = 0 }, false},
		{"zero L1 block time", func(c *L2Config) { c.L1BlockTime = 0 }, false},
		{"fractional K", func(c *L2Config) { c.BlockInterval, c.L1BlockTime = 2, 5 }, false},
		{"one block per L1 block", func(c *L2Config) { c.BlockInterval, c.L1BlockTime = 5, 5 }, false},
		{"L2 slower than L1", func(c *L2Config) { c.BlockInterval, c.L1BlockTime = 10, 5 }, false},
		{"zero gas limit", func(c *L2Config) { c.BlockGasLimit = 0 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			tc.mod(&c)
			err := c.Validate()
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestChainConfigResolvesL2(t *testing.T) {
	Register()
	Register()
	var c chain.Config
	require.NoError(t, json.Unmarshal([]byte(`{"chainId":6290,"l2":`+devnetL2+`}`), &c))
	require.NoError(t, c.ResolveL2())
	require.IsType(t, &L2Config{}, c.L2)
	require.Equal(t, uint64(1), c.L2.(*L2Config).RollupID)

	invalid := chain.Config{L2JSON: json.RawMessage(`{"name":"eez","rollupId":1}`)}
	require.ErrorIs(t, invalid.ResolveL2(), ErrInvalid)
	require.Nil(t, invalid.L2)
}

func TestResolveRules(t *testing.T) {
	c, err := FromJSON([]byte(devnetL2))
	require.NoError(t, err)
	cfg := &chain.Config{ChainID: uint256.NewInt(6290), L2: c}
	rules := (&evmtypes.BlockContext{BlockNumber: 7, Time: 14}).Rules(cfg)
	require.Equal(t, ProtocolVersion, rules.L2Version)

	rules = (&evmtypes.BlockContext{BlockNumber: 7, Time: 14}).Rules(&chain.Config{ChainID: uint256.NewInt(1)})
	require.Zero(t, rules.L2Version)
}
