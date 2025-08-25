package l1infotree

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/zk/contracts"
	"github.com/erigontech/erigon/zk/hermez_db"
	zkTypes "github.com/erigontech/erigon/zk/types"
	"github.com/iden3/go-iden3-crypto/keccak256"
)

type Syncer interface {
	IsSyncStarted() bool
	RunQueryBlocks(lastCheckedBlock uint64)
	GetLogsChan() chan []types.Log
	GetProgressMessageChan() chan string
	GetDoneChan() <-chan struct{}
	GetHeader(blockNumber uint64) (*types.Header, error)
	L1QueryHeaders(logs []types.Log) (map[uint64]*types.Header, error)
	StopQueryBlocks()
	ConsumeQueryBlocks()
	WaitQueryBlocksToFinish()
	QueryForRootLog(to uint64) (*types.Log, error)
	ClearHeaderCache()
}

type L2InfoReaderRpc interface {
	GetExitRootTable(endpoint string) ([]zkTypes.L1InfoTreeUpdate, error)
}

type L2Syncer interface {
	IsSyncStarted() bool
	RunSyncInfoTree() <-chan []zkTypes.L1InfoTreeUpdate
	GetError() error
}

type Updater struct {
	ctx          context.Context
	cfg          *ethconfig.Zk
	syncer       Syncer
	progress     uint64
	latestUpdate *zkTypes.L1InfoTreeUpdate
	l2Syncer     L2Syncer
}

func NewUpdater(ctx context.Context, cfg *ethconfig.Zk, syncer Syncer, l2Syncer L2Syncer) *Updater {
	return &Updater{
		ctx:      ctx,
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

type LogKey struct {
	BlockNumber uint64
	TxIndex     uint
	Index       uint
}

func NewLogKey(l types.Log) LogKey {
	return LogKey{
		BlockNumber: l.BlockNumber,
		TxIndex:     l.TxIndex,
		Index:       l.Index,
	}
}

type L1InfoWorkerPool struct {
	ctx         context.Context
	cancel      context.CancelFunc
	workers     []*L1InfoWorker
	taskChan    chan *L1InfoTask
	taskResChan chan *L1InfoTaskResult
}

func NewL1InfoWorkerPool(ctx context.Context, numWorkers int, syncer Syncer) *L1InfoWorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	pool := &L1InfoWorkerPool{
		ctx:         ctx,
		cancel:      cancel,
		workers:     make([]*L1InfoWorker, numWorkers),
		taskChan:    make(chan *L1InfoTask, numWorkers*1000),
		taskResChan: make(chan *L1InfoTaskResult, numWorkers*1000),
	}

	for i := 0; i < numWorkers; i++ {
		worker := NewL1InfoWorker(ctx, syncer, pool.taskChan, pool.taskResChan)
		pool.workers[i] = worker
	}

	return pool
}
func (pool *L1InfoWorkerPool) Start() {
	for _, worker := range pool.workers {
		go worker.Start()
	}
}
func (pool *L1InfoWorkerPool) Stop() {
	pool.cancel()
}

func (pool *L1InfoWorkerPool) AddTask(task *L1InfoTask) {
	pool.taskChan <- task
}
func (pool *L1InfoWorkerPool) Wait() {
	for _, worker := range pool.workers {
		worker.Wait()
	}
	close(pool.taskResChan)
}

func (pool *L1InfoWorkerPool) GetTaskResChannel() chan *L1InfoTaskResult {
	return pool.taskResChan
}

type L1InfoWorker struct {
	ctx         context.Context
	syncer      Syncer
	taskChan    chan *L1InfoTask
	taskResChan chan *L1InfoTaskResult
	waitGroup   sync.WaitGroup
}

func NewL1InfoWorker(ctx context.Context, syncer Syncer, taskChan chan *L1InfoTask, taskResChan chan *L1InfoTaskResult) *L1InfoWorker {
	return &L1InfoWorker{
		ctx:         ctx,
		syncer:      syncer,
		taskChan:    taskChan,
		taskResChan: taskResChan,
		waitGroup:   sync.WaitGroup{},
	}
}

func (worker *L1InfoWorker) Start() {
	worker.waitGroup.Add(1)
	defer worker.waitGroup.Done()

	for {
		select {
		case <-worker.ctx.Done():
			return
		case task := <-worker.taskChan:
			res := task.Run()
			if res.err != nil {
				log.Debug("Retrying L1 info task", "logKey", res.logKey, "error", res.err)
				// assume a transient error and try again
				worker.waitGroup.Add(1)
				time.AfterFunc(1*time.Second, func() {
					defer worker.waitGroup.Done()

					select {
					case <-worker.ctx.Done():
						// context canceled, don't requeue
						return
					case worker.taskChan <- task:
						log.Debug("Requeued L1 info task", "logKey", res.logKey)
					case <-time.After(10 * time.Second):
						log.Info("Trying to requeue L1 info task", "logKey", res.logKey)
					}
				})
				continue
			}

			// Always report completion (even if no L1InfoTreeUpdate was produced)
			worker.taskResChan <- res
		}
	}
}

func (worker *L1InfoWorker) Wait() {
	worker.waitGroup.Wait()
}

// L1InfoTask is used to query L1 header and create L1InfoTreeUpdate and calculate the leaf hash
type L1InfoTask struct {
	Log    types.Log
	syncer Syncer
}

type L1InfoTaskResult struct {
	l1InfoTreeUpdate *zkTypes.L1InfoTreeUpdateWithLeafHash
	logKey           LogKey
	err              error
}

func NewL1InfoTask(log types.Log, syncer Syncer) *L1InfoTask {
	return &L1InfoTask{
		Log:    log,
		syncer: syncer,
	}
}

func (t *L1InfoTask) Run() *L1InfoTaskResult {
	header, err := t.syncer.GetHeader(t.Log.BlockNumber)
	if err != nil {
		return &L1InfoTaskResult{nil, NewLogKey(t.Log), fmt.Errorf("header not found for block number %d, %w", t.Log.BlockNumber, err)}
	}

	if len(t.Log.Topics) != 3 {
		return &L1InfoTaskResult{nil, NewLogKey(t.Log), nil}
	}

	update, err := createL1InfoTreeUpdateWithLeafHash(t.Log, header)
	if err != nil {
		return &L1InfoTaskResult{nil, NewLogKey(t.Log), fmt.Errorf("createL1InfoTreeUpdateWithLeafHash: %w", err)}
	}
	return &L1InfoTaskResult{l1InfoTreeUpdate: update, logKey: NewLogKey(t.Log), err: nil}
}

func (u *Updater) CheckForInfoTreeUpdates(logPrefix string, tx kv.RwTx) (processed uint64, err error) {
	defer func() {
		if err != nil {
			log.Info(fmt.Sprintf("[%s] CheckForInfoTreeUpdates failed", logPrefix), "err", err)
			u.syncer.StopQueryBlocks()
			u.syncer.ConsumeQueryBlocks()
			u.syncer.WaitQueryBlocksToFinish()
		}
		u.syncer.ClearHeaderCache()
	}()

	hermezDb := hermez_db.NewHermezDb(tx)
	logChan := u.syncer.GetLogsChan()
	progressChan := u.syncer.GetProgressMessageChan()
	doneChan := u.syncer.GetDoneChan()

	workerPool := NewL1InfoWorkerPool(u.ctx, runtime.NumCPU(), u.syncer)
	workerPool.Start()

	taskResChan := workerPool.GetTaskResChannel()

	indexUpdateMap := make(map[LogKey]*zkTypes.L1InfoTreeUpdateWithLeafHash)

	// first get all the logs we need to process
	allLogs := make([]types.Log, 0)

	// Signals that no more logs will arrive
	logsInputDone := make(chan struct{})
	var tasksTotal uint64 = 0
	var expectedUpdates uint64 = 0 // only V1 logs produce updates
	receivedAnyLogs := false

	// track overall activity to avoid blocking forever if nothing happens
	lastActivity := time.Now()

	go func() {
		defer func() {
			// ensure the input-done signal is closed if the function returns
			select {
			case <-logsInputDone:
			default:
				close(logsInputDone)
			}
		}()

		for {
			select {
			case logs := <-logChan:
				if len(logs) == 0 {
					continue
				}
				receivedAnyLogs = true
				lastActivity = time.Now()
				allLogs = append(allLogs, logs...)
				for _, lg := range logs {
					workerPool.AddTask(NewL1InfoTask(lg, u.syncer))
					// Only V1 logs with 3 topics yield an update entry
					if len(lg.Topics) == 3 && lg.Topics[0] == contracts.UpdateL1InfoTreeTopic {
						expectedUpdates++
					}
				}
				tasksTotal += uint64(len(logs))

			case msg := <-progressChan:
				lastActivity = time.Now()
				log.Info(fmt.Sprintf("[%s] %s", logPrefix, msg))

			case <-doneChan:
				// Do not stop the pool here; main loop will stop it after all tasks complete.
				return

			case <-u.ctx.Done():
				log.Info(fmt.Sprintf("[%s] Context canceled while receiving logs", logPrefix))
				return
			}
		}
	}()

	// Drain results until producer finished and every task has reported a result.
	var tasksDone uint64 = 0
	idleTicker := time.NewTicker(5 * time.Second)
	defer idleTicker.Stop()

	// hard cap to avoid waiting forever when syncer is stuck “downloading”
	const noActivityTimeout = 30 * time.Second

drain:
	for {
		select {
		case <-u.ctx.Done():
			log.Info(fmt.Sprintf("[%s] Context canceled, exiting drain loop", logPrefix))
			break drain

		case res := <-taskResChan:
			// Note: taskResChan is not closed until workerPool.Wait() below,
			// so we don't check the 'ok' flag here.
			lastActivity = time.Now()
			tasksDone++
			if res.err != nil {
				// Workers requeue on error, so this should be rare. Keep for safety.
				log.Error(fmt.Sprintf("[%s] Failed to process log (will be requeued by worker): %v", logPrefix, res.err))
				continue
			}
			// Only map successful V1 updates
			if res.l1InfoTreeUpdate != nil {
				indexUpdateMap[res.logKey] = res.l1InfoTreeUpdate
			}

		case <-logsInputDone:
			// No more tasks will be added; wait until all current tasks are done.
			if tasksDone >= tasksTotal {
				log.Debug(fmt.Sprintf("[%s] All tasks done, exiting", logPrefix), "tasksDone", tasksDone, "tasksTotal", tasksTotal)
				break drain
			}

		case <-idleTicker.C:
			// Avoid blocking forever when no logs/results are received.
			if time.Since(lastActivity) > noActivityTimeout {
				log.Warn(fmt.Sprintf("[%s] No activity for %s; exiting drain loop", logPrefix, noActivityTimeout),
					"receivedAnyLogs", receivedAnyLogs,
					"tasksDone", tasksDone, "tasksTotal", tasksTotal)
				break drain
			}
		case <-doneChan:
			if tasksDone >= tasksTotal {
				log.Debug(fmt.Sprintf("[%s] All tasks done, exiting", logPrefix), "tasksDone", tasksDone, "tasksTotal", tasksTotal)
				break drain
			}
		}
	}

	// Stop workers and close result channel
	workerPool.Stop()
	workerPool.Wait()

	// Sanity check: indexUpdateMap should match expected V1 update count
	if uint64(len(indexUpdateMap)) != expectedUpdates {
		log.Warn(fmt.Sprintf("[%s] V1 update results mismatch", logPrefix),
			"expectedV1Updates", expectedUpdates,
			"gotV1Updates", len(indexUpdateMap),
			"tasksTotal", tasksTotal, "tasksDone", tasksDone, "logs", len(allLogs))
	}

	// sort the logs by block number - it is important that we process them in order to get the index correct
	// the v2 topic always appears after the v1 topic so we can rely on this ordering to process the logs correctly.
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

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	processed = 0

	var tree *L1InfoTree
	if len(allLogs) > 0 {
		tree, err = InitialiseL1InfoTree(hermezDb)
		if err != nil {
			return 0, fmt.Errorf("InitialiseL1InfoTree: %w", err)
		}
	}

	for _, l := range allLogs {
		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Processed %d/%d logs, %d%% complete", logPrefix, processed, len(allLogs), int(processed)*100/len(allLogs)))
		default:
		}

		switch l.Topics[0] {
		case contracts.UpdateL1InfoTreeTopic:
			// calculate and store the info tree leaf / index
			update, ok := indexUpdateMap[NewLogKey(l)]
			if !ok {
				log.Error(fmt.Sprintf("[%s] Could not find L1 info tree update for log", logPrefix), "log", l)
			} else {
				if err := u.HandleL1InfoTreeUpdate(hermezDb, tree, update); err != nil {
					return 0, fmt.Errorf("HandleL1InfoTreeUpdate: %w", err)
				}
				processed++
			}
		case contracts.UpdateL1InfoTreeV2Topic:
			// here we can verify that the information we have stored about the info tree is indeed correct
			leafCount := l.Topics[1].Big().Uint64()
			root := l.Data[:32]
			expectedIndex, found, err := hermezDb.GetL1InfoTreeIndexByRoot(common.BytesToHash(root))
			if err != nil {
				return 0, fmt.Errorf("GetL1InfoTreeIndexByRoot: %w", err)
			}
			if !found {
				// some leaf must have been missed in this case as the v2 event from the info tree suggests
				// there is a root that we haven't calculated so we need to unwind the info tree locally
				// and try syncing again
				log.Error(fmt.Sprintf("[%s] Could not find an l1 info tree update by root", logPrefix), "root", common.BytesToHash(root).String())
				if err := u.RollbackL1InfoTree(hermezDb, tx); err != nil {
					return 0, fmt.Errorf("RollbackL1InfoTree: %w", err)
				}
				u.syncer.RunQueryBlocks(u.progress)

				return processed, nil
			}
			if expectedIndex == leafCount-1 {
				if err = hermezDb.WriteConfirmedL1InfoTreeUpdate(expectedIndex, l.BlockNumber); err != nil {
					return 0, fmt.Errorf("WriteConfirmedL1InfoTreeUpdate: %w", err)
				}
			} else {
				log.Error(fmt.Sprintf("[%s] Unexpected index for L1 info tree root", logPrefix), "expected", expectedIndex, "found", leafCount-1)
				if err := u.RollbackL1InfoTree(hermezDb, tx); err != nil {
					return 0, fmt.Errorf("RollbackL1InfoTree: %w", err)
				}
				u.syncer.RunQueryBlocks(u.progress)

				// early return as we need to re-start the syncing process here to get new
				// leaves from scratch
				return processed, nil
			}
			processed++
		default:
			log.Warn("received unexpected topic from l1 info tree stage", "topic", l.Topics[0])
		}
	}

	// save the progress - we add one here so that we don't cause overlap on the next run.  We don't want to duplicate an info tree update in the db
	if len(allLogs) > 0 {
		// here we save progress at the exact block number of the last log.  The syncer will automatically add 1 to this value
		// when the node / syncing process is restarted.
		u.progress = allLogs[len(allLogs)-1].BlockNumber
	}
	if err = stages.SaveStageProgress(tx, stages.L1InfoTree, u.progress); err != nil {
		return 0, fmt.Errorf("SaveStageProgress: %w", err)
	}

	return processed, nil
}

func (u *Updater) HandleL1InfoTreeUpdate(hermezDb *hermez_db.HermezDb, tree *L1InfoTree, update *zkTypes.L1InfoTreeUpdateWithLeafHash) error {
	var err error

	if tree.LeafExists(update.LeafHash) {
		log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", common.BytesToHash(update.LeafHash[:]).String())
		return nil
	}

	if u.latestUpdate != nil {
		update.Index = u.latestUpdate.Index + 1
	}
	// if latestUpdate is nil then Index = 0 which is the default value so no need to set it
	u.latestUpdate = &update.L1InfoTreeUpdate

	newRoot, err := tree.AddLeaf(uint32(u.latestUpdate.Index), update.LeafHash)
	if err != nil {
		return fmt.Errorf("tree.AddLeaf: %w", err)
	}

	log.Debug("New L1 Index",
		"index", u.latestUpdate.Index,
		"root", newRoot.String(),
		"mainnet", u.latestUpdate.MainnetExitRoot.String(),
		"rollup", u.latestUpdate.RollupExitRoot.String(),
		"ger", u.latestUpdate.GER.String(),
		"parent", u.latestUpdate.ParentHash.String(),
	)

	if err = writeL1InfoTreeUpdate(hermezDb, u.latestUpdate, update.LeafHash, newRoot); err != nil {
		return fmt.Errorf("writeL1InfoTreeUpdate: %w", err)
	}

	return nil
}

func (u *Updater) RollbackL1InfoTree(hermezDb *hermez_db.HermezDb, tx kv.RwTx) error {
	log.Debug("Rolling back L1 Info Tree", "progress", u.progress)

	// unexpected confirmedIndex means we missed a leaf somewhere so we need to rollback our data and start re-syncing
	confirmedIndex, confirmedL1BlockNumber, err := hermezDb.GetConfirmedL1InfoTreeUpdate()
	if err != nil {
		return fmt.Errorf("GetConfirmedL1InfoTreeUpdate: %w", err)
	}

	if confirmedIndex == 0 && confirmedL1BlockNumber == 0 {
		// so we know there are no confirmed leaves on the tree at this point
		// now we check if we have an index in the DB or not.  If we do then we
		// are in a state where we've detected fork 12 events but our previously
		// synced info tree is invalid with no way of knowing where / how it became
		// invalid.  Not good, this needs intervention so we just return an error
		// to stop further indexes being used in the network
		latestUpdate, err := hermezDb.GetLatestL1InfoTreeUpdate()
		if err != nil {
			return fmt.Errorf("GetLatestL1InfoTreeUpdate: %w", err)
		}

		if latestUpdate == nil {
			return errors.New("no L1 Info Tree updates found in the DB, cannot reset")
		}

		if latestUpdate.Index > 0 {
			// oh dear
			return errors.New("first fork12 info tree update v2 transaction shows our info tree cannot be verified")
		}

		confirmedL1BlockNumber = u.cfg.L1FirstBlock - 1
	} else {
		// we have some data already so we need to truncate the tables down
		// and reset the latest update data
		if err = truncateL1InfoTreeData(hermezDb, confirmedIndex+1); err != nil {
			return fmt.Errorf("truncateL1InfoTreeData: %w", err)
		}

		// now read the latest confirmed update and set the latest update to this value
		latestUpdate, err := hermezDb.GetL1InfoTreeUpdate(confirmedIndex)
		if err != nil {
			return fmt.Errorf("GetL1InfoTreeUpdate: %w", err)
		}
		u.latestUpdate = latestUpdate
	}

	// stop the syncer
	u.syncer.StopQueryBlocks()
	u.syncer.ConsumeQueryBlocks()
	u.syncer.WaitQueryBlocksToFinish()

	// reset progress for the next iteration
	u.progress = confirmedL1BlockNumber - 1

	if err = stages.SaveStageProgress(tx, stages.L1InfoTree, u.progress); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	log.Debug("L1 Info Tree rollback complete", "progress", u.progress, "confirmedIndex", confirmedIndex, "confirmedL1BlockNumber", confirmedL1BlockNumber)

	return nil
}

func (u *Updater) CheckL2RpcForInfoTreeUpdates(ctx context.Context, logPrefix string, tx kv.RwTx) (infoTrees []zkTypes.L1InfoTreeUpdate, err error) {
	infoTreeChan := u.l2Syncer.RunSyncInfoTree()

LOOP:
	for {
		select {
		case infoTree, ok := <-infoTreeChan:
			if !ok {
				infoTreeChan = nil
				if u.l2Syncer.GetError() != nil {
					log.Warn(fmt.Sprintf("[%s] L2 Info Tree sync failed", logPrefix), "err", u.l2Syncer.GetError())
					return nil, u.l2Syncer.GetError()
				}
				break LOOP
			}
			infoTrees = append(infoTrees, infoTree...)
		case <-ctx.Done():
			log.Info(fmt.Sprintf("[%s] L2 Info Tree sync cancelled", logPrefix))
			return nil, ctx.Err()
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
		case <-ctx.Done():
			log.Info(fmt.Sprintf("[%s] L2 Info Tree sync cancelled", logPrefix))
			return nil, ctx.Err()
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
					log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", common.BytesToHash(leafHash[:]).String())
					continue
				}

				newRoot, err := tree.AddLeaf(uint32(tmpUpdate.Index), leafHash)
				if err != nil {
					return nil, fmt.Errorf("tree.AddLeaf: %w", err)
				}

				if err = writeL1InfoTreeUpdate(hermezDb, tmpUpdate, leafHash, newRoot); err != nil {
					return nil, fmt.Errorf("writeL1InfoTreeUpdate: %w", err)
				}

				processed++
			}
		}

		u.latestUpdate = &update

		leafHash := HashLeafData(u.latestUpdate.GER, u.latestUpdate.ParentHash, u.latestUpdate.Timestamp)
		if tree.LeafExists(leafHash) {
			log.Warn("Skipping log as L1 Info Tree leaf already exists", "hash", common.BytesToHash(leafHash[:]).String())
			continue
		}

		newRoot, err := tree.AddLeaf(uint32(u.latestUpdate.Index), leafHash)
		if err != nil {
			return nil, fmt.Errorf("tree.AddLeaf: %w", err)
		}

		if err = writeL1InfoTreeUpdate(hermezDb, u.latestUpdate, leafHash, newRoot); err != nil {
			return nil, fmt.Errorf("writeL1InfoTreeUpdate: %w", err)
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

func createL1InfoTreeUpdateWithLeafHash(l types.Log, header *types.Header) (*zkTypes.L1InfoTreeUpdateWithLeafHash, error) {
	update, err := createL1InfoTreeUpdate(l, header)
	if err != nil {
		return nil, err
	}

	leafHash := HashLeafData(update.GER, update.ParentHash, update.Timestamp)
	return &zkTypes.L1InfoTreeUpdateWithLeafHash{
		L1InfoTreeUpdate: *update,
		LeafHash:         leafHash,
	}, nil
}

func writeL1InfoTreeUpdate(
	hermezDb *hermez_db.HermezDb,
	update *zkTypes.L1InfoTreeUpdate,
	leafHash [32]byte,
	newRoot [32]byte,
) error {
	var err error

	if err = hermezDb.WriteL1InfoTreeUpdate(update); err != nil {
		return fmt.Errorf("WriteL1InfoTreeUpdate: %w", err)
	}
	if err = hermezDb.WriteL1InfoTreeUpdateToGer(update); err != nil {
		return fmt.Errorf("WriteL1InfoTreeUpdateToGer: %w", err)
	}
	if err = hermezDb.WriteL1InfoTreeLeaf(update.Index, leafHash); err != nil {
		return fmt.Errorf("WriteL1InfoTreeLeaf: %w", err)
	}
	if err = hermezDb.WriteL1InfoTreeRoot(common.BytesToHash(newRoot[:]), update.Index); err != nil {
		return fmt.Errorf("WriteL1InfoTreeRoot: %w", err)
	}
	return nil
}

func truncateL1InfoTreeData(hermezDb *hermez_db.HermezDb, fromIndex uint64) error {
	if err := hermezDb.TruncateL1InfoTreeUpdates(fromIndex); err != nil {
		return fmt.Errorf("TruncateL1InfoTreeUpdates: %w", err)
	}
	if err := hermezDb.TruncateL1InfoTreeUpdatesByGer(fromIndex); err != nil {
		return fmt.Errorf("TruncateL1InfoTreeUpdatesByGer: %w", err)
	}
	if err := hermezDb.TruncateL1InfoTreeLeaves(fromIndex); err != nil {
		return fmt.Errorf("TruncateL1InfoTreeLeaves: %w", err)
	}
	if err := hermezDb.TruncateL1InfoTreeRoots(fromIndex); err != nil {
		return fmt.Errorf("TruncateL1InfoTreeRoots: %w", err)
	}
	return nil
}
