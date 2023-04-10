package ethconsensusconfig

import (
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/ethash/ethashcfg"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/consensusconfig"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdallgrpc"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/db"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

func CreateConsensusEngine(chainConfig *chain.Config, logger log.Logger, config interface{}, notify []string, noverify bool, HeimdallgRPCAddress string, HeimdallURL string, WithoutHeimdall bool, datadir string, snapshots *snapshotsync.RoSnapshots, readonly bool, chainDb ...kv.RwDB) consensus.Engine {
	var eng consensus.Engine

	switch consensusCfg := config.(type) {
	case *ethashcfg.Config:
		switch consensusCfg.PowMode {
		case ethashcfg.ModeFake:
			log.Warn("Ethash used in fake mode")
			eng = ethash.NewFaker()
		case ethashcfg.ModeTest:
			log.Warn("Ethash used in test mode")
			eng = ethash.NewTester(nil, noverify)
		case ethashcfg.ModeShared:
			log.Warn("Ethash used in shared mode")
			eng = ethash.NewShared()
		default:
			eng = ethash.New(ethashcfg.Config{
				CachesInMem:      consensusCfg.CachesInMem,
				CachesLockMmap:   consensusCfg.CachesLockMmap,
				DatasetDir:       consensusCfg.DatasetDir,
				DatasetsInMem:    consensusCfg.DatasetsInMem,
				DatasetsOnDisk:   consensusCfg.DatasetsOnDisk,
				DatasetsLockMmap: consensusCfg.DatasetsLockMmap,
			}, notify, noverify)
		}
	case *params.ConsensusSnapshotConfig:
		if chainConfig.Clique != nil {
			if consensusCfg.DBPath == "" {
				consensusCfg.DBPath = filepath.Join(datadir, "clique", "db")
			}
			eng = clique.New(chainConfig, consensusCfg, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory, readonly))
		}
	case *chain.AuRaConfig:
		if chainConfig.Aura != nil {
			if consensusCfg.DBPath == "" {
				consensusCfg.DBPath = filepath.Join(datadir, "aura")
			}
			var err error
			eng, err = aura.NewAuRa(chainConfig.Aura, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory, readonly), chainConfig.Aura.Etherbase, consensusconfig.GetConfigByChain(chainConfig.ChainName))
			if err != nil {
				panic(err)
			}
		}
	case *chain.ParliaConfig:
		if chainConfig.Parlia != nil {
			if consensusCfg.DBPath == "" {
				consensusCfg.DBPath = filepath.Join(datadir, "parlia")
			}
			eng = parlia.New(chainConfig, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory, readonly), snapshots, chainDb[0])
		}
	case *chain.BorConfig:
		// If Matic bor consensus is requested, set it up
		// In order to pass the ethereum transaction tests, we need to set the burn contract which is in the bor config
		// Then, bor != nil will also be enabled for ethash and clique. Only enable Bor for real if there is a validator contract present.
		if chainConfig.Bor != nil && chainConfig.Bor.ValidatorContract != "" {
			genesisContractsClient := contract.NewGenesisContractsClient(chainConfig, chainConfig.Bor.ValidatorContract, chainConfig.Bor.StateReceiverContract)
			spanner := span.NewChainSpanner(contract.ValidatorSet(), chainConfig)
			borDbPath := filepath.Join(datadir, "bor") // bor consensus path: datadir/bor
			db := db.OpenDatabase(borDbPath, logger, false, readonly)

			var heimdallClient bor.IHeimdallClient
			if WithoutHeimdall {
				return bor.New(chainConfig, db, spanner, nil, genesisContractsClient)
			} else {
				if HeimdallgRPCAddress != "" {
					heimdallClient = heimdallgrpc.NewHeimdallGRPCClient(HeimdallgRPCAddress)
				} else {
					heimdallClient = heimdall.NewHeimdallClient(HeimdallURL)
				}
				eng = bor.New(chainConfig, db, spanner, heimdallClient, genesisContractsClient)
			}
		}
	}

	if eng == nil {
		panic("unknown config" + spew.Sdump(config))
	}

	if chainConfig.TerminalTotalDifficulty == nil {
		return eng
	} else {
		return serenity.New(eng) // the Merge
	}
}
