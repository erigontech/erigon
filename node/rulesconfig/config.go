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

package rulesconfig

import (
	"context"
	"sync"

	"github.com/davecgh/go-spew/spew"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/aura"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borabi"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

// L2EngineFunc builds an L2 stack's rules engine for a chain configured
// with that stack's L2Config. Registered by the L2 package at init time and
// consulted by CreateRulesEngine before the built-in type switch.
type L2EngineFunc func(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine

var (
	l2EnginesMu sync.RWMutex
	l2Engines   = map[string]L2EngineFunc{}
)

// RegisterL2Engine registers an L2 stack's rules-engine constructor under its
// L2Config.Name(). Panics on an empty name, a nil constructor, or a name that
// is already registered — all programming errors caught at init time.
func RegisterL2Engine(name string, newEngine L2EngineFunc) {
	if name == "" {
		panic("rulesconfig: RegisterL2Engine: empty name")
	}
	if newEngine == nil {
		panic("rulesconfig: RegisterL2Engine: nil constructor for " + name)
	}
	l2EnginesMu.Lock()
	defer l2EnginesMu.Unlock()
	if _, exists := l2Engines[name]; exists {
		panic("L2 rules engine already registered: " + name)
	}
	l2Engines[name] = newEngine
}

func unregisterL2Engine(name string) {
	l2EnginesMu.Lock()
	defer l2EnginesMu.Unlock()
	delete(l2Engines, name)
}

func l2Engine(name string) (L2EngineFunc, bool) {
	l2EnginesMu.RLock()
	defer l2EnginesMu.RUnlock()
	newEngine, ok := l2Engines[name]
	return newEngine, ok
}

func CreateRulesEngine(ctx context.Context, nodeConfig *nodecfg.Config, chainConfig *chain.Config, config any, noVerify bool,
	withoutHeimdall bool, blockReader services.FullBlockReader, readonly bool,
	logger log.Logger, polygonBridge *bridge.Service, heimdallService *heimdall.Service,
) rules.Engine {
	var eng rules.Engine

	if chainConfig.L2 != nil {
		newEngine, ok := l2Engine(chainConfig.L2.Name())
		if !ok {
			panic("no L2 rules engine registered for: " + chainConfig.L2.Name())
		}
		eng = newEngine(ctx, chainConfig, logger)
	} else {
		switch consensusCfg := config.(type) {
		case *ethashcfg.Config:
			switch consensusCfg.PowMode {
			case ethashcfg.ModeFake:
				logger.Warn("Ethash used in fake mode")
				eng = ethash.NewFaker()
			case ethashcfg.ModeTest:
				logger.Warn("Ethash used in test mode")
				eng = ethash.NewTester(noVerify)
			case ethashcfg.ModeShared:
				logger.Warn("Ethash used in shared mode")
				eng = ethash.NewShared()
			default:
				eng = ethash.New(ethashcfg.Config{
					CachesInMem:      consensusCfg.CachesInMem,
					CachesLockMmap:   consensusCfg.CachesLockMmap,
					DatasetDir:       consensusCfg.DatasetDir,
					DatasetsInMem:    consensusCfg.DatasetsInMem,
					DatasetsOnDisk:   consensusCfg.DatasetsOnDisk,
					DatasetsLockMmap: consensusCfg.DatasetsLockMmap,
				}, noVerify)
			}
		case *chain.AuRaConfig:
			if chainConfig.Aura != nil {
				var err error
				var db kv.RwDB

				db, err = node.OpenDatabase(ctx, nodeConfig, dbcfg.ConsensusDB, "aura", readonly, logger)

				if err != nil {
					panic(err)
				}

				eng, err = aura.NewAuRa(chainConfig.Aura, db)
				if err != nil {
					panic(err)
				}
			}
		case *borcfg.BorConfig:
			// If Matic bor consensus is requested, set it up
			// In order to pass the ethereum transaction tests, we need to set the burn contract which is in the bor config
			// Then, bor != nil will also be enabled for ethash. Only enable Bor for real if there is a validator contract present.
			if chainConfig.Bor != nil && consensusCfg.ValidatorContract != "" {
				stateReceiver := bor.NewStateReceiver(consensusCfg.StateReceiverContractAddress())
				spanner := bor.NewChainSpanner(borabi.ValidatorSetContractABI(), chainConfig, withoutHeimdall, logger)
				eng = bor.New(chainConfig, blockReader, spanner, stateReceiver, logger, polygonBridge, heimdallService)
			}
		}
	}

	if eng == nil {
		panic("unknown config" + spew.Sdump(config))
	}

	if chainConfig.TerminalTotalDifficulty == nil {
		return eng
	} else {
		return merge.New(eng) // the Merge
	}
}

func CreateRulesEngineBareBones(ctx context.Context, chainConfig *chain.Config, logger log.Logger) rules.Engine {
	var consensusConfig any

	if chainConfig.Aura != nil {
		consensusConfig = chainConfig.Aura
	} else if chainConfig.Bor != nil {
		consensusConfig = chainConfig.Bor
	} else if chainConfig.L2 != nil {
		consensusConfig = chainConfig.L2
	} else {
		var ethashCfg ethashcfg.Config
		ethashCfg.PowMode = ethashcfg.ModeFake
		consensusConfig = &ethashCfg
	}

	return CreateRulesEngine(ctx, &nodecfg.Config{}, chainConfig, consensusConfig, true, /* noVerify */
		true /* withoutHeimdall */, nil /* blockReader */, false /* readonly */, logger, nil, nil)
}
