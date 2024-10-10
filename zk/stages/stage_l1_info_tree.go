package stages

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	"github.com/ledgerwatch/log/v3"
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
	logger log.Logger,
) (funcErr error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Info Tree stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Info Tree stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		var err error
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

	latestUpdate, _, err := hermezDb.GetLatestL1InfoTreeUpdate()
	if err != nil {
		return err
	}

	if !cfg.syncer.IsSyncStarted() {
		cfg.syncer.RunQueryBlocks(progress)
		defer func() {
			if funcErr != nil {
				cfg.syncer.StopQueryBlocks()
				cfg.syncer.ConsumeQueryBlocks()
				cfg.syncer.WaitQueryBlocksToFinish()
			}
		}()
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
			time.Sleep(10 * time.Millisecond)
		}
	}

	// sort the logs by block number - it is important that we process them in order to get the index correct
	sort.Slice(allLogs, func(i, j int) bool {
		l1 := allLogs[i]
		l2 := allLogs[j]
		// first sort by block number and if equal then by tx index
		if l1.BlockNumber != l2.BlockNumber {
			return l1.BlockNumber < l2.BlockNumber
		}
		if l1.TxIndex != l2.TxIndex {
			return l1.TxIndex < l2.TxIndex
		}
		return l1.Index < l2.Index
	})

	// chunk the logs into batches, so we don't overload the RPC endpoints too much at once
	chunks := chunkLogs(allLogs, 50)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	processed := 0

	tree, err := initialiseL1InfoTree(hermezDb)
	if err != nil {
		funcErr = err
		return funcErr
	}

	// process the logs in chunks
	for _, chunk := range chunks {
		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Processed %d/%d logs, %d%% complete", logPrefix, processed, len(allLogs), processed*100/len(allLogs)))
		default:
		}

		headersMap, err := cfg.syncer.L1QueryHeaders(chunk)
		if err != nil {
			funcErr = err
			return funcErr
		}

		for _, l := range chunk {
			switch l.Topics[0] {
			case contracts.UpdateL1InfoTreeTopic:
				header := headersMap[l.BlockNumber]
				if header == nil {
					header, funcErr = cfg.syncer.GetHeader(l.BlockNumber)
					if funcErr != nil {
						return funcErr
					}
				}

				tmpUpdate, err := CreateL1InfoTreeUpdate(l, header)
				if err != nil {
					funcErr = err
					return funcErr
				}

				leafHash := l1infotree.HashLeafData(tmpUpdate.GER, tmpUpdate.ParentHash, tmpUpdate.Timestamp)
				if tree.LeafExists(leafHash) {
					log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", leafHash)
					continue
				}

				if latestUpdate != nil {
					tmpUpdate.Index = latestUpdate.Index + 1
				} // if latestUpdate is nil then Index = 0 which is the default value so no need to set it
				latestUpdate = tmpUpdate

				newRoot, err := tree.AddLeaf(uint32(latestUpdate.Index), leafHash)
				if err != nil {
					funcErr = err
					return funcErr
				}
				log.Debug("New L1 Index",
					"index", latestUpdate.Index,
					"root", newRoot.String(),
					"mainnet", latestUpdate.MainnetExitRoot.String(),
					"rollup", latestUpdate.RollupExitRoot.String(),
					"ger", latestUpdate.GER.String(),
					"parent", latestUpdate.ParentHash.String(),
				)

				if funcErr = HandleL1InfoTreeUpdate(hermezDb, latestUpdate); funcErr != nil {
					return funcErr
				}
				if funcErr = hermezDb.WriteL1InfoTreeLeaf(latestUpdate.Index, leafHash); funcErr != nil {
					return funcErr
				}
				if funcErr = hermezDb.WriteL1InfoTreeRoot(common.BytesToHash(newRoot[:]), latestUpdate.Index); funcErr != nil {
					return funcErr
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
	if funcErr = stages.SaveStageProgress(tx, stages.L1InfoTree, progress); funcErr != nil {
		return funcErr
	}

	log.Info(fmt.Sprintf("[%s] Info tree updates", logPrefix), "count", len(allLogs))

	if freshTx {
		if funcErr = tx.Commit(); funcErr != nil {
			return funcErr
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

func initialiseL1InfoTree(hermezDb *hermez_db.HermezDb) (*l1infotree.L1InfoTree, error) {
	leaves, err := hermezDb.GetAllL1InfoTreeLeaves()
	if err != nil {
		return nil, err
	}

	allLeaves := make([][32]byte, len(leaves))
	for i, l := range leaves {
		allLeaves[i] = l
	}

	tree, err := l1infotree.NewL1InfoTree(32, allLeaves)
	if err != nil {
		return nil, err
	}

	return tree, nil
}

func UnwindL1InfoTreeStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1InfoTreeCfg, ctx context.Context) error {
	return nil
}

func PruneL1InfoTreeStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1InfoTreeCfg, ctx context.Context) error {
	return nil
}
