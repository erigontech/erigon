package l1infotree

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	zkTypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"
)

type Syncer interface {
	IsSyncStarted() bool
	RunQueryBlocks(lastCheckedBlock uint64)
	GetLogsChan() chan []types.Log
	GetProgressMessageChan() chan string
	IsDownloading() bool
	GetHeader(blockNumber uint64) (*types.Header, error)
	L1QueryHeaders(logs []types.Log) (map[uint64]*types.Header, error)
	StopQueryBlocks()
	ConsumeQueryBlocks()
	WaitQueryBlocksToFinish()
	QueryForRootLog(to uint64) (*types.Log, error)
}

type L2InfoReaderRpc interface {
	GetExitRootTable(endpoint string) ([]zkTypes.L1InfoTreeUpdate, error)
}

type L2Syncer interface {
	IsSyncStarted() bool
	IsSyncFinished() bool
	GetInfoTreeChan() chan []zkTypes.L1InfoTreeUpdate
	RunSyncInfoTree()
	ConsumeInfoTree()
}

type Updater struct {
	cfg          *ethconfig.Zk
	syncer       Syncer
	progress     uint64
	latestUpdate *zkTypes.L1InfoTreeUpdate
	l2Syncer     L2Syncer
}

func NewUpdater(cfg *ethconfig.Zk, syncer Syncer, l2Syncer L2Syncer) *Updater {
	return &Updater{
		cfg:      cfg,
		syncer:   syncer,
		l2Syncer: l2Syncer,
	}
}

func (u *Updater) GetProgress() uint64 {
	return u.progress
}

func (u *Updater) GetLatestUpdate() *zkTypes.L1InfoTreeUpdate {
	return u.latestUpdate
}

func (u *Updater) WarmUp(tx kv.RwTx) (err error) {
	defer func() {
		if err != nil {
			u.syncer.StopQueryBlocks()
			u.syncer.ConsumeQueryBlocks()
			u.syncer.WaitQueryBlocksToFinish()
		}
	}()

	hermezDb := hermez_db.NewHermezDb(tx)

	progress, err := stages.GetStageProgress(tx, stages.L1InfoTree)
	if err != nil {
		return err
	}
	if progress == 0 {
		progress = u.cfg.L1FirstBlock - 1
	}

	u.progress = progress

	latestUpdate, err := hermezDb.GetLatestL1InfoTreeUpdate()
	if err != nil {
		return err
	}

	u.latestUpdate = latestUpdate

	if !u.syncer.IsSyncStarted() {
		u.syncer.RunQueryBlocks(u.progress)
	}

	return nil
}

func (u *Updater) CheckForInfoTreeUpdates(logPrefix string, tx kv.RwTx) (allLogs []types.Log, err error) {
	defer func() {
		if err != nil {
			u.syncer.StopQueryBlocks()
			u.syncer.ConsumeQueryBlocks()
			u.syncer.WaitQueryBlocksToFinish()
		}
	}()

	hermezDb := hermez_db.NewHermezDb(tx)
	logChan := u.syncer.GetLogsChan()
	progressChan := u.syncer.GetProgressMessageChan()

	// first get all the logs we need to process
LOOP:
	for {
		select {
		case logs := <-logChan:
			allLogs = append(allLogs, logs...)
		case msg := <-progressChan:
			log.Info(fmt.Sprintf("[%s] %s", logPrefix, msg))
		default:
			if !u.syncer.IsDownloading() {
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

	var tree *L1InfoTree
	if len(allLogs) > 0 {
		log.Info(fmt.Sprintf("[%s] Checking for L1 info tree updates, logs count:%v", logPrefix, len(allLogs)))
		tree, err = InitialiseL1InfoTree(hermezDb)
		if err != nil {
			return nil, fmt.Errorf("InitialiseL1InfoTree: %w", err)
		}
	}

	// process the logs in chunks
	for _, chunk := range chunks {
		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Processed %d/%d logs, %d%% complete", logPrefix, processed, len(allLogs), processed*100/len(allLogs)))
		default:
		}

		headersMap, err := u.syncer.L1QueryHeaders(chunk)
		if err != nil {
			return nil, fmt.Errorf("L1QueryHeaders: %w", err)
		}

		for _, l := range chunk {
			switch l.Topics[0] {
			case contracts.UpdateL1InfoTreeTopic:
				header := headersMap[l.BlockNumber]
				if header == nil {
					header, err = u.syncer.GetHeader(l.BlockNumber)
					if err != nil {
						return nil, fmt.Errorf("GetHeader: %w", err)
					}
				}

				tmpUpdate, err := createL1InfoTreeUpdate(l, header)
				if err != nil {
					return nil, fmt.Errorf("createL1InfoTreeUpdate: %w", err)
				}

				leafHash := HashLeafData(tmpUpdate.GER, tmpUpdate.ParentHash, tmpUpdate.Timestamp)
				if tree.LeafExists(leafHash) {
					log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", leafHash)
					continue
				}

				if u.latestUpdate != nil {
					tmpUpdate.Index = u.latestUpdate.Index + 1
				} // if latestUpdate is nil then Index = 0 which is the default value so no need to set it
				u.latestUpdate = tmpUpdate

				newRoot, err := tree.AddLeaf(uint32(u.latestUpdate.Index), leafHash)
				if err != nil {
					return nil, fmt.Errorf("tree.AddLeaf: %w", err)
				}
				log.Debug("New L1 Index",
					"index", u.latestUpdate.Index,
					"root", newRoot.String(),
					"mainnet", u.latestUpdate.MainnetExitRoot.String(),
					"rollup", u.latestUpdate.RollupExitRoot.String(),
					"ger", u.latestUpdate.GER.String(),
					"parent", u.latestUpdate.ParentHash.String(),
				)

				if err = handleL1InfoTreeUpdate(hermezDb, u.latestUpdate); err != nil {
					return nil, fmt.Errorf("handleL1InfoTreeUpdate: %w", err)
				}
				if err = hermezDb.WriteL1InfoTreeLeaf(u.latestUpdate.Index, leafHash); err != nil {
					return nil, fmt.Errorf("WriteL1InfoTreeLeaf: %w", err)
				}
				if err = hermezDb.WriteL1InfoTreeRoot(common.BytesToHash(newRoot[:]), u.latestUpdate.Index); err != nil {
					return nil, fmt.Errorf("WriteL1InfoTreeRoot: %w", err)
				}

				processed++
			default:
				log.Warn("received unexpected topic from l1 info tree stage", "topic", l.Topics[0])
			}
		}
	}

	// save the progress - we add one here so that we don't cause overlap on the next run.  We don't want to duplicate an info tree update in the db
	if len(allLogs) > 0 {
		u.progress = allLogs[len(allLogs)-1].BlockNumber + 1
	}
	if err = stages.SaveStageProgress(tx, stages.L1InfoTree, u.progress); err != nil {
		return nil, fmt.Errorf("SaveStageProgress: %w", err)
	}

	return allLogs, nil
}

func (u *Updater) CheckL2RpcForInfoTreeUpdates(logPrefix string, tx kv.RwTx) (infoTrees []zkTypes.L1InfoTreeUpdate, err error) {
	u.l2Syncer.RunSyncInfoTree()
	go u.l2Syncer.ConsumeInfoTree()

	infoTreeChan := u.l2Syncer.GetInfoTreeChan()

LOOP:
	for {
		select {
		case infoTree := <-infoTreeChan:
			infoTrees = append(infoTrees, infoTree...)
		default:
			if u.l2Syncer.IsSyncFinished() {
				log.Info(fmt.Sprintf("[%s] Received %v L2 Info Tree updates", logPrefix, len(infoTrees)))
				break LOOP
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// ok we have all the info tree updates from the l2, now we need to process them
	sort.Slice(infoTrees, func(i, j int) bool {
		return infoTrees[i].Index < infoTrees[j].Index
	})

	hermezDb := hermez_db.NewHermezDb(tx)
	tree, err := InitialiseL1InfoTree(hermezDb)
	if err != nil {
		return nil, fmt.Errorf("InitialiseL1InfoTree: %w", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	processed := 0

	for _, update := range infoTrees {
		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Processed %d/%d info tree updates from L2 RPC, %d%% complete", logPrefix, processed, len(infoTrees), processed*100/len(infoTrees)))
		default:
		}

		// create root
		if u.latestUpdate == nil {
			// query for root log by:
			// (from: 0) > (to: update index 1 block number)
			// then return logs[0]
			rootLog, err := u.syncer.QueryForRootLog(update.BlockNumber)
			if err != nil {
				return nil, fmt.Errorf("QueryForRootLog: %w", err)
			}

			switch rootLog.Topics[0] {
			case contracts.UpdateL1InfoTreeTopic:
				header, err := u.syncer.GetHeader(rootLog.BlockNumber)
				if err != nil {
					return nil, fmt.Errorf("GetHeader: %w", err)
				}

				tmpUpdate, err := createL1InfoTreeUpdate(*rootLog, header)
				if err != nil {
					return nil, fmt.Errorf("createL1InfoTreeUpdate: %w", err)
				}
				tmpUpdate.Index = 0

				leafHash := HashLeafData(tmpUpdate.GER, tmpUpdate.ParentHash, tmpUpdate.Timestamp)
				if tree.LeafExists(leafHash) {
					log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", leafHash)
					continue
				}

				newRoot, err := tree.AddLeaf(uint32(tmpUpdate.Index), leafHash)
				if err != nil {
					return nil, fmt.Errorf("tree.AddLeaf: %w", err)
				}

				if err = handleL1InfoTreeUpdate(hermezDb, tmpUpdate); err != nil {
					return nil, fmt.Errorf("handleL1InfoTreeUpdate: %w", err)
				}

				if err = hermezDb.WriteL1InfoTreeLeaf(tmpUpdate.Index, leafHash); err != nil {
					return nil, fmt.Errorf("WriteL1InfoTreeLeaf: %w", err)
				}

				if err = hermezDb.WriteL1InfoTreeRoot(common.BytesToHash(newRoot[:]), tmpUpdate.Index); err != nil {
					return nil, fmt.Errorf("WriteL1InfoTreeRoot: %w", err)
				}

				processed++
			}
		}

		u.latestUpdate = &update

		leafHash := HashLeafData(u.latestUpdate.GER, u.latestUpdate.ParentHash, u.latestUpdate.Timestamp)
		if tree.LeafExists(leafHash) {
			log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", leafHash)
			continue
		}

		newRoot, err := tree.AddLeaf(uint32(u.latestUpdate.Index), leafHash)
		if err != nil {
			return nil, fmt.Errorf("tree.AddLeaf: %w", err)
		}

		if err = handleL1InfoTreeUpdate(hermezDb, u.latestUpdate); err != nil {
			return nil, fmt.Errorf("handleL1InfoTreeUpdate: %w", err)
		}

		if err = hermezDb.WriteL1InfoTreeLeaf(u.latestUpdate.Index, leafHash); err != nil {
			return nil, fmt.Errorf("WriteL1InfoTreeLeaf: %w", err)
		}

		if err = hermezDb.WriteL1InfoTreeRoot(common.BytesToHash(newRoot[:]), u.latestUpdate.Index); err != nil {
			return nil, fmt.Errorf("WriteL1InfoTreeRoot: %w", err)
		}

		processed++
	}

	if len(infoTrees) > 0 {
		u.progress = infoTrees[len(infoTrees)-1].BlockNumber + 1
	}

	if err = stages.SaveStageProgress(tx, stages.L1InfoTree, u.progress); err != nil {
		return nil, fmt.Errorf("SaveStageProgress: %w", err)
	}

	return infoTrees, nil
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

func InitialiseL1InfoTree(hermezDb *hermez_db.HermezDb) (*L1InfoTree, error) {
	leaves, err := hermezDb.GetAllL1InfoTreeLeaves()
	if err != nil {
		return nil, fmt.Errorf("GetAllL1InfoTreeLeaves: %w", err)
	}

	allLeaves := make([][32]byte, len(leaves))
	for i, l := range leaves {
		allLeaves[i] = l
	}

	tree, err := NewL1InfoTree(32, allLeaves)
	if err != nil {
		return nil, fmt.Errorf("NewL1InfoTree: %w", err)
	}

	return tree, nil
}

func createL1InfoTreeUpdate(l types.Log, header *types.Header) (*zkTypes.L1InfoTreeUpdate, error) {
	if len(l.Topics) != 3 {
		return nil, errors.New("received log for info tree that did not have 3 topics")
	}

	if l.BlockNumber != header.Number.Uint64() {
		return nil, errors.New("received log for info tree that did not match the block number")
	}

	mainnetExitRoot := l.Topics[1]
	rollupExitRoot := l.Topics[2]
	combined := append(mainnetExitRoot.Bytes(), rollupExitRoot.Bytes()...)
	ger := keccak256.Hash(combined)
	update := &zkTypes.L1InfoTreeUpdate{
		GER:             common.BytesToHash(ger),
		MainnetExitRoot: mainnetExitRoot,
		RollupExitRoot:  rollupExitRoot,
		BlockNumber:     l.BlockNumber,
		Timestamp:       header.Time,
		ParentHash:      header.ParentHash,
	}

	return update, nil
}

func handleL1InfoTreeUpdate(
	hermezDb *hermez_db.HermezDb,
	update *zkTypes.L1InfoTreeUpdate,
) error {
	var err error
	if err = hermezDb.WriteL1InfoTreeUpdate(update); err != nil {
		return fmt.Errorf("WriteL1InfoTreeUpdate: %w", err)
	}
	if err = hermezDb.WriteL1InfoTreeUpdateToGer(update); err != nil {
		return fmt.Errorf("WriteL1InfoTreeUpdateToGer: %w", err)
	}
	return nil
}
