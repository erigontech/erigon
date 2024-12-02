package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	eristate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	zkUtils "github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/erigon/zk/witness"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

type WitnessDb interface {
}

type WitnessCfg struct {
	db             kv.RwDB
	zkCfg          *ethconfig.Zk
	chainConfig    *chain.Config
	engine         consensus.Engine
	blockReader    services.FullBlockReader
	agg            *eristate.Aggregator
	historyV3      bool
	dirs           datadir.Dirs
	forcedContracs []common.Address
}

func StageWitnessCfg(db kv.RwDB, zkCfg *ethconfig.Zk, chainConfig *chain.Config, engine consensus.Engine, blockReader services.FullBlockReader, agg *eristate.Aggregator, historyV3 bool, dirs datadir.Dirs, forcedContracs []common.Address) WitnessCfg {
	cfg := WitnessCfg{
		db:             db,
		zkCfg:          zkCfg,
		chainConfig:    chainConfig,
		engine:         engine,
		blockReader:    blockReader,
		agg:            agg,
		historyV3:      historyV3,
		dirs:           dirs,
		forcedContracs: forcedContracs,
	}

	return cfg
}

// ///////////////////////////////////////////
// 1. Check to which block it should calculate witnesses
// 2. Unwind to that block
// 3. Calculate witnesses up to current executed block
// 4. Delete old block witnesses
// ////////////////////////////////////////////
func SpawnStageWitness(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg WitnessCfg,
) error {
	logPrefix := s.LogPrefix()
	if cfg.zkCfg.WitnessCacheLimit == 0 {
		log.Info(fmt.Sprintf("[%s] Skipping witness cache stage. Cache not set or limit is set to 0", logPrefix))
		return nil
	}
	log.Info(fmt.Sprintf("[%s] Starting witness cache stage", logPrefix))
	if sequencer.IsSequencer() {
		log.Info(fmt.Sprintf("[%s] skipping -- sequencer", logPrefix))
		return nil
	}
	defer log.Info(fmt.Sprintf("[%s] Finished witness cache stage", logPrefix))

	freshTx := false
	if tx == nil {
		freshTx = true
		log.Debug(fmt.Sprintf("[%s] no tx provided, creating a new one", logPrefix))
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw, %w", err)
		}
		defer tx.Rollback()
	}

	stageWitnessProgressBlockNo, err := stages.GetStageProgress(tx, stages.Witness)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	stageInterhashesProgressBlockNo, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	if stageInterhashesProgressBlockNo <= stageWitnessProgressBlockNo {
		log.Info(fmt.Sprintf("[%s] Skipping stage, no new blocks", logPrefix))
		return nil
	}

	unwindPoint := stageWitnessProgressBlockNo
	if stageInterhashesProgressBlockNo-cfg.zkCfg.WitnessCacheLimit > unwindPoint {
		unwindPoint = stageInterhashesProgressBlockNo - cfg.zkCfg.WitnessCacheLimit
	}

	//get unwind point to be end of previous batch
	hermezDb := hermez_db.NewHermezDb(tx)
	blocks, err := getBlocks(tx, unwindPoint, stageInterhashesProgressBlockNo)
	if err != nil {
		return fmt.Errorf("getBlocks: %w", err)
	}

	// generator := witness.NewGenerator(cfg.dirs, cfg.historyV3, cfg.agg, cfg.blockReader, cfg.chainConfig, cfg.zkCfg, cfg.engine)
	memTx := membatchwithdb.NewMemoryBatchWithSize(tx, cfg.dirs.Tmp, cfg.zkCfg.WitnessMemdbSize)
	defer memTx.Rollback()
	if err := zkUtils.PopulateMemoryMutationTables(memTx); err != nil {
		return fmt.Errorf("PopulateMemoryMutationTables: %w", err)
	}
	memHermezDb := hermez_db.NewHermezDbReader(memTx)

	log.Info(fmt.Sprintf("[%s] Unwinding tree and hashess for witness generation", logPrefix), "from", unwindPoint, "to", stageInterhashesProgressBlockNo)
	if err := witness.UnwindForWitness(ctx, memTx, unwindPoint, stageInterhashesProgressBlockNo, cfg.dirs, cfg.historyV3, cfg.agg); err != nil {
		return fmt.Errorf("UnwindForWitness: %w", err)
	}
	log.Info(fmt.Sprintf("[%s] Unwind done", logPrefix))
	startBlock := blocks[0].NumberU64()

	prevHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, startBlock-1)
	if err != nil {
		return fmt.Errorf("blockReader.HeaderByNumber: %w", err)
	}

	getHeader := func(hash common.Hash, number uint64) *eritypes.Header {
		h, e := cfg.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}

	reader := state.NewPlainState(tx, blocks[0].NumberU64(), systemcontracts.SystemContractCodeLookup[cfg.chainConfig.ChainName])
	defer reader.Close()
	prevStateRoot := prevHeader.Root

	log.Info(fmt.Sprintf("[%s] Executing blocks and collecting witnesses", logPrefix), "from", startBlock, "to", stageInterhashesProgressBlockNo)

	now := time.Now()
	for _, block := range blocks {
		reader.SetBlockNr(block.NumberU64())
		tds := state.NewTrieDbState(prevHeader.Root, tx, startBlock-1, nil)
		tds.SetResolveReads(true)
		tds.StartNewBuffer()
		tds.SetStateReader(reader)

		trieStateWriter := tds.NewTrieStateWriter()
		if err := witness.PrepareGersForWitness(block, memHermezDb, tds, trieStateWriter); err != nil {
			return fmt.Errorf("PrepareGersForWitness: %w", err)
		}

		getHashFn := core.GetHashFn(block.Header(), getHeader)

		chainReader := stagedsync.NewChainReaderImpl(cfg.chainConfig, tx, nil, log.New())

		vmConfig := vm.Config{}
		if _, err = core.ExecuteBlockEphemerallyZk(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, tds, trieStateWriter, chainReader, nil, hermezDb, &prevStateRoot); err != nil {
			return fmt.Errorf("ExecuteBlockEphemerallyZk: %w", err)
		}

		prevStateRoot = block.Root()

		w, err := witness.BuildWitnessFromTrieDbState(ctx, memTx, tds, reader, cfg.forcedContracs, false)
		if err != nil {
			return fmt.Errorf("BuildWitnessFromTrieDbState: %w", err)
		}

		bytes, err := witness.GetWitnessBytes(w, false)
		if err != nil {
			return fmt.Errorf("GetWitnessBytes: %w", err)
		}

		if hermezDb.WriteWitnessCache(block.NumberU64(), bytes); err != nil {
			return fmt.Errorf("WriteWitnessCache: %w", err)
		}
		if time.Since(now) > 10*time.Second {
			log.Info(fmt.Sprintf("[%s] Executing blocks and collecting witnesses", logPrefix), "block", block.NumberU64())
			now = time.Now()
		}
	}
	log.Info(fmt.Sprintf("[%s] Witnesses collected", logPrefix))

	// delete cache for blocks lower than the limit
	log.Info(fmt.Sprintf("[%s] Deleting old witness caches", logPrefix))
	if err := hermezDb.DeleteWitnessCaches(0, stageInterhashesProgressBlockNo-cfg.zkCfg.WitnessCacheLimit); err != nil {
		return fmt.Errorf("DeleteWitnessCache: %w", err)
	}

	if err := stages.SaveStageProgress(tx, stages.Witness, stageInterhashesProgressBlockNo); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "lastBlockNumber", stageInterhashesProgressBlockNo)

	if freshTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}

	return nil
}

func getBlocks(tx kv.Tx, startBlock, endBlock uint64) (blocks []*eritypes.Block, err error) {
	idx := 0
	blocks = make([]*eritypes.Block, endBlock-startBlock+1)
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		block, err := rawdb.ReadBlockByNumber(tx, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ReadBlockByNumber: %w", err)
		}
		blocks[idx] = block
		idx++
	}

	return blocks, nil
}

func UnwindWitnessStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg WitnessCfg, ctx context.Context) (err error) {
	logPrefix := u.LogPrefix()
	if cfg.zkCfg.WitnessCacheLimit == 0 {
		log.Info(fmt.Sprintf("[%s] Skipping witness cache stage. Cache not set or limit is set to 0", logPrefix))
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		if tx, err = cfg.db.BeginRw(ctx); err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	if cfg.zkCfg.WitnessCacheLimit == 0 {
		log.Info(fmt.Sprintf("[%s] Skipping witness cache stage. Cache not set or limit is set to 0", logPrefix))
		return nil
	}

	fromBlock := u.UnwindPoint + 1
	toBlock := u.CurrentBlockNumber
	log.Info(fmt.Sprintf("[%s] Unwinding witness cache stage from block number", logPrefix), "fromBlock", fromBlock, "toBlock", toBlock)
	defer log.Info(fmt.Sprintf("[%s] Unwinding witness cache complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)
	if err := hermezDb.DeleteWitnessCaches(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteWitnessCache: %w", err)
	}

	if err := stages.SaveStageProgress(tx, stages.Witness, fromBlock); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("u.Done: %w", err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}
	return nil
}

func PruneWitnessStage(s *stagedsync.PruneState, tx kv.RwTx, cfg WitnessCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()
	if cfg.zkCfg.WitnessCacheLimit == 0 {
		log.Info(fmt.Sprintf("[%s] Skipping witness cache stage. Cache not set or limit is set to 0", logPrefix))
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	log.Info(fmt.Sprintf("[%s] Pruning witnes caches...", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Pruning witnes caches complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)

	toBlock, err := stages.GetStageProgress(tx, stages.Witness)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	if err := hermezDb.DeleteWitnessCaches(0, toBlock); err != nil {
		return fmt.Errorf("DeleteWitnessCache: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "stageProgress", 0)
	if err := stages.SaveStageProgress(tx, stages.Witness, 0); err != nil {
		return fmt.Errorf("SaveStageProgress: %v", err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}
	return nil
}
