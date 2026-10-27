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
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
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
	require.Equal(t, Rollup0, rules.L2Version)

	rules = (&evmtypes.BlockContext{BlockNumber: 7, Time: 14}).Rules(&chain.Config{ChainID: uint256.NewInt(1)})
	require.Zero(t, rules.L2Version)
}

// eezDevL2 is the l2 section of eez.dev: rollupX is config.rollupX of
// fixture eezdev-699069 (eez/testdata/SOURCES.md).
const eezDevL2 = `{"name":"eez","rollupId":1,"l1ChainId":10200,"eezRegistry":"0x96d6b9a4ad7ce2ea5ac80ce7f2ecef1e26c8fa33","registryDeployBlock":23413742,"blockInterval":1,"l1BlockTime":5,"blockGasLimit":30000000,
"rollupX":{"blockIntervalSeconds":1,"syncIntervalBlocks":5,"l1Reference":{"chainId":10200,"number":23413703,"hash":"0x5ac534c090ac3b1383dd8a7647a9df20af2a358dec370e708e1c48bd7d6488ad","timestamp":1791397980,"prevRandao":"0x1af463d6327b8c6b5a5bb581238f0b136d2b93bce04a35b9672734f0383f44ee"}}}`

func TestRollupX(t *testing.T) {
	c, err := FromJSON([]byte(eezDevL2))
	require.NoError(t, err)
	require.Equal(t, &RollupXConfig{
		BlockIntervalSeconds: 1,
		SyncIntervalBlocks:   5,
		L1Reference: L1Reference{
			ChainID:    10200,
			Number:     23413703,
			Hash:       common.HexToHash("0x5ac534c090ac3b1383dd8a7647a9df20af2a358dec370e708e1c48bd7d6488ad"),
			Timestamp:  1791397980,
			PrevRandao: common.HexToHash("0x1af463d6327b8c6b5a5bb581238f0b136d2b93bce04a35b9672734f0383f44ee"),
		},
	}, c.RollupX)
	require.Equal(t, RollupX, c.RuleSet())
	require.Equal(t, common.HexToAddress("0x4B2760F84B83d6b3829E30A7573B0172B6d6402C"), c.EEZL2())
	enc, err := json.Marshal(c)
	require.NoError(t, err)
	require.JSONEq(t, eezDevL2, string(enc))

	cfg := &chain.Config{ChainID: uint256.NewInt(699069), L2: c}
	rules := (&evmtypes.BlockContext{BlockNumber: 7, Time: 14}).Rules(cfg)
	require.Equal(t, RollupX, rules.L2Version)

	devnet, err := FromJSON([]byte(devnetL2))
	require.NoError(t, err)
	require.Equal(t, Rollup0, devnet.RuleSet())
	require.Equal(t, common.HexToAddress("0x4200000000000000000000000000000000000007"), devnet.EEZL2())
}

func TestRollupXValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*L2Config)
	}{
		{"zero interval", func(c *L2Config) { c.RollupX.BlockIntervalSeconds = 0 }},
		{"one position", func(c *L2Config) { c.RollupX.SyncIntervalBlocks, c.L1BlockTime = 1, 1 }},
		{"Chiado schedule", func(c *L2Config) { c.RollupX.SyncIntervalBlocks, c.L1BlockTime = 6, 6 }},
		{"Ethereum schedule", func(c *L2Config) { c.L1ChainID, c.RollupX.L1Reference.ChainID = 1, 1 }},
		{"other L1", func(c *L2Config) { c.RollupX.L1Reference.ChainID = 7331 }},
		{"zero reference hash", func(c *L2Config) { c.RollupX.L1Reference.Hash = common.Hash{} }},
		{"block interval", func(c *L2Config) { c.BlockInterval, c.L1BlockTime = 2, 10 }},
		{"L1 block time", func(c *L2Config) { c.L1BlockTime = 10 }},
		{"gas limit", func(c *L2Config) { c.BlockGasLimit = 36_000_000 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := FromJSON([]byte(eezDevL2))
			require.NoError(t, err)
			tc.mod(c)
			require.ErrorIs(t, c.Validate(), ErrInvalid)
		})
	}
	_, err := FromJSON([]byte(strings.Replace(eezDevL2, `"syncIntervalBlocks":5`, `"syncIntervalBlocks":5,"blockIntervalMillis":1000`, 1)))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestValidateChain(t *testing.T) {
	rollupX, err := FromJSON([]byte(eezDevL2))
	require.NoError(t, err)
	rollup0, err := FromJSON([]byte(devnetL2))
	require.NoError(t, err)
	valid := func() *chain.Config {
		cfg := chain.TestChainOsakaConfig.Copy()
		cfg.BaseFeeChangeDenominator = new(RollupXBaseFeeChangeDenominator)
		return cfg
	}
	require.NoError(t, rollupX.ValidateChain(valid()))
	require.NoError(t, rollup0.ValidateChain(chain.TestChainOsakaConfig))
	require.ErrorIs(t, rollup0.ValidateChain(valid()), ErrInvalid)
	for _, tc := range []struct {
		name string
		mod  func(*chain.Config)
	}{
		{"default denominator", func(c *chain.Config) { c.BaseFeeChangeDenominator = nil }},
		{"fee collector", func(c *chain.Config) { c.FeeCollector = &common.Address{1} }},
		{"Osaka later", func(c *chain.Config) { c.OsakaTime = common.NewUint64(1) }},
		{"Amsterdam", func(c *chain.Config) { c.AmsterdamTime = common.NewUint64(100) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid()
			tc.mod(cfg)
			require.ErrorIs(t, rollupX.ValidateChain(cfg), ErrInvalid)
		})
	}
}

func TestCheckGenesis(t *testing.T) {
	c, err := FromJSON([]byte(eezDevL2))
	require.NoError(t, err)
	// The fixture genesis has the reference timestamp.
	genesis := func() *types.Header {
		return &types.Header{Time: 0x6ac6905c, MixDigest: c.RollupX.L1Reference.PrevRandao}
	}
	require.NoError(t, c.RollupX.CheckGenesis(genesis()))
	for _, mod := range []func(*types.Header){
		func(h *types.Header) { h.Time++ },
		func(h *types.Header) { h.Time = c.RollupX.L1Reference.Timestamp - 5 },
		func(h *types.Header) { h.MixDigest = common.Hash{} },
		func(h *types.Header) { h.Number.SetOne() },
	} {
		h := genesis()
		mod(h)
		require.ErrorIs(t, c.RollupX.CheckGenesis(h), ErrInvalid)
	}
}
