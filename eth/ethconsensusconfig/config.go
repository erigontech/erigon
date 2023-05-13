package ethconsensusconfig

import (
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdallgrpc"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/db"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/ethash/ethashcfg"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/params"
)

func CreateConsensusEngine(chainConfig *chain.Config, config interface{}, notify []string, noVerify bool,
	heimdallGrpcAddress string, heimdallUrl string, withoutHeimdall bool, dataDir string, readonly bool,
	logger log.Logger,
) consensus.Engine {
	var eng consensus.Engine

	switch consensusCfg := config.(type) {
	case *ethashcfg.Config:
		switch consensusCfg.PowMode {
		case ethashcfg.ModeFake:
			logger.Warn("Ethash used in fake mode")
			eng = ethash.NewFaker()
		case ethashcfg.ModeTest:
			logger.Warn("Ethash used in test mode")
			eng = ethash.NewTester(nil, noVerify)
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
			}, notify, noVerify)
		}
	case *params.ConsensusSnapshotConfig:
		if chainConfig.Clique != nil {
			if consensusCfg.DBPath == "" {
				consensusCfg.DBPath = filepath.Join(dataDir, "clique", "db")
			}
			eng = clique.New(chainConfig, consensusCfg, db.OpenDatabase(consensusCfg.DBPath, consensusCfg.InMemory, readonly))
		}
	case *chain.AuRaConfig:
		if chainConfig.Aura != nil {
			dbPath := filepath.Join(dataDir, "aura")
			var err error
			eng, err = aura.NewAuRa(chainConfig.Aura, db.OpenDatabase(dbPath, false, readonly))
			if err != nil {
				panic(err)
			}
		}
	case *chain.BorConfig:
		// If Matic bor consensus is requested, set it up
		// In order to pass the ethereum transaction tests, we need to set the burn contract which is in the bor config
		// Then, bor != nil will also be enabled for ethash and clique. Only enable Bor for real if there is a validator contract present.
		if chainConfig.Bor != nil && chainConfig.Bor.ValidatorContract != "" {
			genesisContractsClient := contract.NewGenesisContractsClient(chainConfig, chainConfig.Bor.ValidatorContract, chainConfig.Bor.StateReceiverContract, logger)
			spanner := span.NewChainSpanner(contract.ValidatorSet(), chainConfig, logger)
			borDbPath := filepath.Join(dataDir, "bor") // bor consensus path: datadir/bor
			db := db.OpenDatabase(borDbPath, false, readonly)

			var heimdallClient bor.IHeimdallClient
			if withoutHeimdall {
				return bor.New(chainConfig, db, spanner, nil, genesisContractsClient, logger)
			} else {
				if heimdallGrpcAddress != "" {
					heimdallClient = heimdallgrpc.NewHeimdallGRPCClient(heimdallGrpcAddress)
				} else {
					heimdallClient = heimdall.NewHeimdallClient(heimdallUrl)
				}
				eng = bor.New(chainConfig, db, spanner, heimdallClient, genesisContractsClient, logger)
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

func CreateConsensusEngineBareBones(chainConfig *chain.Config, logger log.Logger) consensus.Engine {
	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = params.CliqueSnapshot
	} else if chainConfig.Aura != nil {
		consensusConfig = &chainConfig.Aura
	} else if chainConfig.Bor != nil {
		consensusConfig = &chainConfig.Bor
	} else {
		var ethashCfg ethashcfg.Config
		ethashCfg.PowMode = ethashcfg.ModeFake
		consensusConfig = &ethashCfg
	}

	return CreateConsensusEngine(chainConfig, consensusConfig, nil /* notify */, true, /* noVerify */
		"" /* heimdallGrpcAddress */, "" /* heimdallUrl */, true /* withoutHeimdall */, "" /*dataDir*/, false /* readonly */, logger)
}
