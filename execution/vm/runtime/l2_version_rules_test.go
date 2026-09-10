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

package runtime

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

func newL2TestConfig(t *testing.T, chainID uint64) *Config {
	t.Helper()
	versionMap := state.NewVersionMap(nil)
	st := state.NewWithVersionMap(state.NewVersionedStateReader(0, state.ReadSet{}, versionMap, state.NewNoopReader()), versionMap)
	t.Cleanup(st.Close)
	st.SetTxContext(1, -1)

	cfg := &Config{
		ChainConfig: &chain.Config{
			ChainID:               uint256.NewInt(chainID),
			HomesteadBlock:        new(uint64),
			TangerineWhistleBlock: new(uint64),
			SpuriousDragonBlock:   new(uint64),
			ByzantiumBlock:        new(uint64),
			ConstantinopleBlock:   new(uint64),
			PetersburgBlock:       new(uint64),
			IstanbulBlock:         new(uint64),
			MuirGlacierBlock:      new(uint64),
			BerlinBlock:           new(uint64),
			LondonBlock:           new(uint64),
			ArrowGlacierBlock:     new(uint64),
			GrayGlacierBlock:      new(uint64),
			ShanghaiTime:          new(uint64),
			CancunTime:            new(uint64),
			PragueTime:            new(uint64),
			OsakaTime:             new(uint64),
			AmsterdamTime:         new(uint64),
		},
		Origin: accounts.InternAddress(common.HexToAddress("0xcafe")),
		State:  st,
	}
	setDefaults(cfg)
	return cfg
}

type l2VersionRules struct{}

func (l2VersionRules) Name() string { return "test-l2" }

func (l2VersionRules) ResolveRules(l2Version, _, _ uint64, rules *chain.Rules) {
	rules.L2Version = l2Version
}

// runtime.Execute and runtime.Call start the tracer themselves, so a partial
// VMContext there is invisible to a test that drives OnTxStart directly. The
// 4byte, flat-call and JS tracers rebuild Rules from this context: a dropped
// L2Version misclassifies a version-gated precompile, and a nil ChainConfig
// panics them outright.
func TestRuntimeStartsTracerWithFullVMContext(t *testing.T) {
	const chainID = 900434
	const activeAt = 30

	newCfg := func(t *testing.T) (*Config, *tracing.VMContext) {
		t.Helper()
		cfg := newL2TestConfig(t, chainID)
		cfg.ChainConfig.L2 = l2VersionRules{}
		cfg.L2Version = activeAt
		var got tracing.VMContext
		cfg.EVMConfig.Tracer = &tracing.Hooks{
			OnTxStart: func(vmctx *tracing.VMContext, _ types.Transaction, _ accounts.Address) {
				if vmctx != nil {
					got = *vmctx
				}
			},
		}
		return cfg, &got
	}

	assertFull := func(t *testing.T, got *tracing.VMContext) {
		t.Helper()
		require.NotNil(t, got.IntraBlockState, "the tracer must be started at all")
		require.NotNil(t, got.ChainConfig, "a nil ChainConfig panics the 4byte and flat tracers")
		require.NotNil(t, got.Rules, "a dropped rule set evaluates version-gated providers at 0")
		require.Equal(t, uint64(activeAt), got.Rules.L2Version,
			"the traced rules must be the ones the EVM resolved")
	}

	t.Run("Call", func(t *testing.T) {
		cfg, got := newCfg(t)
		_, _, err := Call(accounts.InternAddress(common.BytesToAddress([]byte{0x9b})), nil, cfg)
		require.NoError(t, err)
		assertFull(t, got)
	})

	t.Run("Execute", func(t *testing.T) {
		cfg, got := newCfg(t)
		_, _, err := Execute([]byte{byte(vm.STOP)}, nil, cfg, t.TempDir())
		require.NoError(t, err)
		assertFull(t, got)
	})
}
