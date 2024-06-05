package stages

import (
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"fmt"
	"github.com/ledgerwatch/log/v3"
	"context"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"sort"
	"time"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	"github.com/gateway-fm/cdk-erigon-lib/common"
)

type L1InfoTreeCfg struct {
	db     kv.RwDB
	zkCfg  *ethconfig.Zk
	syncer IL1Syncer
}

func StageL1InfoTreeCfg(db kv.RwDB, zkCfg *ethconfig.Zk, sync IL1Syncer) L1InfoTreeCfg {
	return L1InfoTreeCfg{
		db:     db,
		zkCfg:  zkCfg,
		syncer: sync,
	}
}

func SpawnL1InfoTreeStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	cfg L1InfoTreeCfg,
	ctx context.Context,
	initialCycle bool,
	quiet bool,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Info Tree stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Info Tree stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	progress, err := stages.GetStageProgress(tx, stages.L1InfoTree)
	if err != nil {
		return err
	}
	if progress == 0 {
		progress = cfg.zkCfg.L1FirstBlock - 1
	}

	latestUpdate, found, err := hermezDb.GetLatestL1InfoTreeUpdate()
	if err != nil {
		return err
	}

	if !cfg.syncer.IsSyncStarted() {
		cfg.syncer.Run(progress)
	}

	logChan := cfg.syncer.GetLogsChan()
	progressChan := cfg.syncer.GetProgressMessageChan()

	// first get all the logs we need to process
	var allLogs []types.Log
LOOP:
	for {
		select {
		case logs := <-logChan:
			allLogs = append(allLogs, logs...)
		case msg := <-progressChan:
			log.Info(fmt.Sprintf("[%s] %s", logPrefix, msg))
		default:
			if !cfg.syncer.IsDownloading() {
				break LOOP
			}
		}
	}

	// sort the logs by block number - it is important that we process them in order to get the index correct
	sort.Slice(allLogs, func(i, j int) bool {
		return allLogs[i].BlockNumber < allLogs[j].BlockNumber
	})

	// chunk the logs into batches, so we don't overload the RPC endpoints too much at once
	chunks := chunkLogs(allLogs, 50)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	processed := 0

	var tree *l1infotree.L1InfoTree
	var allLeaves [][32]byte
	treeInitialised := false

	// process the logs in chunks
	for _, chunk := range chunks {
		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Processed %d/%d logs, %d%% complete", logPrefix, processed, len(allLogs), processed*100/len(allLogs)))
		default:
		}

		headersMap, err := cfg.syncer.L1QueryHeaders(chunk)
		if err != nil {
			return err
		}

		for _, l := range chunk {
			header := headersMap[l.BlockNumber]
			switch l.Topics[0] {
			case contracts.UpdateL1InfoTreeTopic:
				if !treeInitialised {
					tree, allLeaves, err = initialiseL1InfoTree(hermezDb)
					if err != nil {
						return err
					}
					treeInitialised = true
				}

				latestUpdate, err = HandleL1InfoTreeUpdate(cfg.syncer, hermezDb, l, latestUpdate, found, header)
				if err != nil {
					return err
				}
				found = true

				leafHash := l1infotree.HashLeafData(latestUpdate.GER, latestUpdate.ParentHash, latestUpdate.Timestamp)

				err = hermezDb.WriteL1InfoTreeLeaf(latestUpdate.Index, leafHash)
				if err != nil {
					return err
				}

				// we do not want to add index 0 to the tree
				allLeaves = append(allLeaves, leafHash)

				newRoot, err := tree.BuildL1InfoRoot(allLeaves)
				if err != nil {
					return err
				}

				err = hermezDb.WriteL1InfoTreeRoot(common.BytesToHash(newRoot[:]), latestUpdate.Index)
				if err != nil {
					return err
				}

				processed++
			default:
				log.Warn("received unexpected topic from l1 info tree stage", "topic", l.Topics[0])
			}
		}
	}

	// save the progress - we add one here so that we don't cause overlap on the next run.  We don't want to duplicate an info tree update in the db
	if len(allLogs) > 0 {
		progress = allLogs[len(allLogs)-1].BlockNumber + 1
	}
	if err := stages.SaveStageProgress(tx, stages.L1InfoTree, progress); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Info tree updates", logPrefix), "count", len(allLogs))

	if freshTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func chunkLogs(slice []types.Log, chunkSize int) [][]types.Log {
	var chunks [][]types.Log
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// If end is greater than the length of the slice, reassign it to the length of the slice
		if end > len(slice) {
			end = len(slice)
		}

		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

func initialiseL1InfoTree(hermezDb *hermez_db.HermezDb) (*l1infotree.L1InfoTree, [][32]byte, error) {
	leaves, err := hermezDb.GetAllL1InfoTreeLeaves()
	if err != nil {
		return nil, nil, err
	}

	allLeaves := make([][32]byte, len(leaves))
	for i, l := range leaves {
		allLeaves[i] = l
	}

	tree, err := l1infotree.NewL1InfoTree(32, allLeaves)
	if err != nil {
		return nil, nil, err
	}

	return tree, allLeaves, nil
}

func UnwindL1InfoTreeStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1InfoTreeCfg, ctx context.Context) error {
	return nil
}

func PruneL1InfoTreeStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1InfoTreeCfg, ctx context.Context) error {
	return nil
}
