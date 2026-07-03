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

package cocoon

import (
	"context"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/rulesconfig"
)

func init() {
	rulesconfig.RegisterL2Engine("cocoon", func(context.Context, *chain.Config, log.Logger) rules.Engine {
		return &Engine{}
	})
}

// Enable wires cfg for the cocoon sovereign-chain stack: it sets cfg.L2 to a
// Config keyed on cfg.ChainID and, when precompiles is non-nil, registers it
// as that chain's precompile provider.
func Enable(cfg *chain.Config, precompiles vm.PrecompilesFunc) {
	if cfg.ChainID == nil {
		panic("cocoon: Enable: chain.Config.ChainID is nil")
	}
	chainID := cfg.ChainID.Uint64()
	cfg.L2 = Config{ChainID: chainID}
	if precompiles != nil {
		vm.RegisterPrecompiles(chainID, precompiles)
	}
}

// DevChainConfig returns an all-current-forks chain.Config for a cocoon
// sovereign chain identified by chainID, with L2 pre-set as Enable would
// (without a precompile provider).
func DevChainConfig(chainID uint64) *chain.Config {
	cfg := &chain.Config{
		ChainName:                     "cocoon-dev",
		ChainID:                       uint256.NewInt(chainID),
		Rules:                         chain.CocoonRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		AmsterdamTime:                 common.NewUint64(0),
	}
	Enable(cfg, nil)
	return cfg
}
