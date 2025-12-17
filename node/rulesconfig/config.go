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
	"path/filepath"

	"github.com/davecgh/go-spew/spew"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/aura"
	"github.com/erigontech/erigon/execution/protocol/rules/clique"
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

func CreateRulesEngine(ctx context.Context, nodeConfig *nodecfg.Config, chainConfig *chain.Config, config interface{}, noVerify bool,
	withoutHeimdall bool, blockReader services.FullBlockReader, readonly bool,
	logger log.Logger, polygonBridge *bridge.Service, heimdallService *heimdall.Service,
) rules.Engine {
	var eng rules.Engine

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
	case *chainspec.ConsensusSnapshotConfig:
		if chainConfig.Clique != nil {
			if consensusCfg.InMemory {
				nodeConfig.Dirs.DataDir = ""
			} else {
				if consensusCfg.DBPath != "" {
					if filepath.Base(consensusCfg.DBPath) == "clique" {
						nodeConfig.Dirs.DataDir = filepath.Dir(consensusCfg.DBPath)
					} else {
						nodeConfig.Dirs.DataDir = consensusCfg.DBPath
					}
				}
			}

			var err error
			var db kv.RwDB

			db, err = node.OpenDatabase(ctx, nodeConfig, dbcfg.ConsensusDB, "clique", readonly, logger)

			if err != nil {
				panic(err)
			}

			eng = clique.New(chainConfig, consensusCfg, db, logger)
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
		// Then, bor != nil will also be enabled for ethash and clique. Only enable Bor for real if there is a validator contract present.
		if chainConfig.Bor != nil && consensusCfg.ValidatorContract != "" {
			stateReceiver := bor.NewStateReceiver(consensusCfg.StateReceiverContractAddress())
			spanner := bor.NewChainSpanner(borabi.ValidatorSetContractABI(), chainConfig, withoutHeimdall, logger)
			eng = bor.New(chainConfig, blockReader, spanner, stateReceiver, logger, polygonBridge, heimdallService)
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
	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = chainspec.CliqueSnapshot
	} else if chainConfig.Aura != nil {
		consensusConfig = chainConfig.Aura
	} else if chainConfig.Bor != nil {
		consensusConfig = chainConfig.Bor
	} else {
		var ethashCfg ethashcfg.Config
		ethashCfg.PowMode = ethashcfg.ModeFake
		consensusConfig = &ethashCfg
	}

	return CreateRulesEngine(ctx, &nodecfg.Config{}, chainConfig, consensusConfig, true, /* noVerify */
		true /* withoutHeimdall */, nil /* blockReader */, false /* readonly */, logger, nil, nil)
}
