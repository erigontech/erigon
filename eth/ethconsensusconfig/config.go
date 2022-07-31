package ethconsensusconfig

import (
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/consensusconfig"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/db"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

func CreateConsensusEngine(chainConfig *params.ChainConfig, logger log.Logger, config interface{}, notify []string, noverify bool, HeimdallURL string, WithoutHeimdall bool, datadir string, snapshots *snapshotsync.RoSnapshots, readonly bool) consensus.Engine {
	var eng consensus.Engine

	switch consensusCfg := config.(type) {
	case *ethash.Config:
		switch consensusCfg.PowMode {
		case ethash.ModeFake:
			log.Warn("Ethash used in fake mode")
			eng = ethash.NewFaker()
		case ethash.ModeTest:
			log.Warn("Ethash used in test mode")
			eng = ethash.NewTester(nil, noverify)
		case ethash.ModeShared:
			log.Warn("Ethash used in shared mode")
			eng = ethash.NewShared()
		default:
			eng = ethash.New(ethash.Config{
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
			eng = clique.New(chainConfig, consensusCfg, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory, readonly))
		}
	case *params.AuRaConfig:
		if chainConfig.Aura != nil {
			var err error
			eng, err = aura.NewAuRa(chainConfig.Aura, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory, readonly), chainConfig.Aura.Etherbase, consensusconfig.GetConfigByChain(chainConfig.ChainName))
			if err != nil {
				panic(err)
			}
		}
	case *params.ParliaConfig:
		if chainConfig.Parlia != nil {
			eng = parlia.New(chainConfig, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory, readonly), snapshots)
		}
	case *params.BorConfig:
		if chainConfig.Bor != nil {
			borDbPath := filepath.Join(datadir, "bor") // bor consensus path: datadir/bor
			eng = bor.New(chainConfig, db.OpenDatabase(borDbPath, logger, false, readonly), HeimdallURL, WithoutHeimdall)
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
