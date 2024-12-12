package stagedsync

///go:build integration

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal"
	statelib "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/params"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
)

type OpType int

const readType = 0
const writeType = 1
const otherType = 2
const greenTick = "✅"
const redCross = "❌"

const threeRockets = "🚀🚀🚀"

type Op struct {
	key      state.VersionKey
	duration time.Duration
	opType   OpType
	val      int
}

type testExecTask struct {
	*exec.TxTask
	ops          []Op
	readMap      map[state.VersionKey]state.VersionedRead
	writeMap     map[state.VersionKey]state.VersionedWrite
	sender       common.Address
	nonce        int
	dependencies []int
}

type PathGenerator func(addr common.Address, i int, j int, total int) state.VersionKey

type TaskRunner func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration)

type TaskRunnerWithMetadata func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration)

type Timer func(txIdx int, opIdx int) time.Duration

type Sender func(int) common.Address

func NewTestExecTask(txIdx int, ops []Op, sender common.Address, nonce int) *testExecTask {
	return &testExecTask{
		TxTask: &exec.TxTask{
			BlockNum: 1,
			TxNum:    1 + uint64(txIdx),
			TxIndex:  txIdx,
		},
		ops:          ops,
		readMap:      make(map[state.VersionKey]state.VersionedRead),
		writeMap:     make(map[state.VersionKey]state.VersionedWrite),
		sender:       sender,
		nonce:        nonce,
		dependencies: []int{},
	}
}

func sleep(i time.Duration) {
	start := time.Now()
	for time.Since(start) < i {
	}
}

func (t *testExecTask) Execute(evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3,
	ibs *state.IntraBlockState,
	stateWriter *state.StateWriterV3,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	isMining bool) *exec.Result {
	// Sleep for 50 microsecond to simulate setup time
	sleep(time.Microsecond * 50)

	version := t.Version()

	t.readMap = make(map[state.VersionKey]state.VersionedRead)
	t.writeMap = make(map[state.VersionKey]state.VersionedWrite)

	deps := -1

	for i, op := range t.ops {
		k := op.key

		switch op.opType {
		case readType:
			if _, ok := t.writeMap[k]; ok {
				sleep(op.duration)
				continue
			}

			result := ibs.ReadVersion(k, version.TxIndex)

			val := result.Value()

			if i == 0 && val != nil && (val.(int) != t.nonce) {
				return &exec.Result{Err: exec.ErrExecAbortError{}}
			}

			if result.Status() == state.MVReadResultDependency {
				if result.DepIdx() > deps {
					deps = result.DepIdx()
				}
			}

			var readKind int

			if result.Status() == state.MVReadResultDone {
				readKind = state.ReadKindMap
			} else if result.Status() == state.MVReadResultNone {
				readKind = state.ReadKindStorage
			}

			sleep(op.duration)

			t.readMap[k] = state.VersionedRead{Path: k, Kind: readKind, V: state.Version{TxIndex: result.DepIdx(), Incarnation: result.Incarnation()}}
		case writeType:
			t.writeMap[k] = state.VersionedWrite{Path: k, V: version, Val: op.val}
		case otherType:
			sleep(op.duration)
		default:
			panic(fmt.Sprintf("Unknown op type: %d", op.opType))
		}
	}

	if deps != -1 {
		return &exec.Result{Err: exec.ErrExecAbortError{Dependency: deps, OriginError: fmt.Errorf("Dependency error")}}
	}

	return &exec.Result{
		StateDb: ibs,
	}
}

func (t *testExecTask) MVWriteList() []state.VersionedWrite {
	return t.MVFullWriteList()
}

func (t *testExecTask) MVFullWriteList() []state.VersionedWrite {
	writes := make([]state.VersionedWrite, 0, len(t.writeMap))

	for _, v := range t.writeMap {
		writes = append(writes, v)
	}

	return writes
}

func (t *testExecTask) MVReadList() []state.VersionedRead {
	reads := make([]state.VersionedRead, 0, len(t.readMap))

	for _, v := range t.readMap {
		reads = append(reads, v)
	}

	return reads
}

func (t *testExecTask) settle() {}

func (t *testExecTask) Sender() common.Address {
	return t.sender
}

func (t *testExecTask) Hash() common.Hash {
	return common.BytesToHash([]byte(fmt.Sprintf("%d", t.TxIndex)))
}

func (t *testExecTask) Dependencies() []int {
	return t.dependencies
}

func randTimeGenerator(min time.Duration, max time.Duration) func(txIdx int, opIdx int) time.Duration {
	return func(txIdx int, opIdx int) time.Duration {
		return time.Duration(rand.Int63n(int64(max-min))) + min
	}
}

func longTailTimeGenerator(min time.Duration, max time.Duration, i int, j int) func(txIdx int, opIdx int) time.Duration {
	return func(txIdx int, opIdx int) time.Duration {
		if txIdx%i == 0 && opIdx == j {
			return max * 100
		} else {
			return time.Duration(rand.Int63n(int64(max-min))) + min
		}
	}
}

var randomPathGenerator = func(sender common.Address, i int, j int, total int) state.VersionKey {
	return state.VersionStateKey(common.BigToAddress((big.NewInt(int64(i % 10)))), common.BigToHash((big.NewInt(int64(total)))))
}

var dexPathGenerator = func(sender common.Address, i int, j int, total int) state.VersionKey {
	if j == total-1 || j == 2 {
		return state.VersionSubpathKey(common.BigToAddress(big.NewInt(int64(0))), 1)
	} else {
		return state.VersionSubpathKey(common.BigToAddress(big.NewInt(int64(j))), 1)
	}
}

var readTime = randTimeGenerator(4*time.Microsecond, 12*time.Microsecond)
var writeTime = randTimeGenerator(2*time.Microsecond, 6*time.Microsecond)
var nonIOTime = randTimeGenerator(1*time.Microsecond, 2*time.Microsecond)

func taskFactory(numTask int, sender Sender, readsPerT int, writesPerT int, nonIOPerT int, pathGenerator PathGenerator, readTime Timer, writeTime Timer, nonIOTime Timer) ([]exec.Task, time.Duration) {
	exec := make([]exec.Task, 0, numTask)

	var serialDuration time.Duration

	senderNonces := make(map[common.Address]int)

	for i := 0; i < numTask; i++ {
		s := sender(i)

		// Set first two ops to always read and write nonce
		ops := make([]Op, 0, readsPerT+writesPerT+nonIOPerT)

		ops = append(ops, Op{opType: readType, key: state.VersionSubpathKey(s, 2), duration: readTime(i, 0), val: senderNonces[s]})

		senderNonces[s]++

		ops = append(ops, Op{opType: writeType, key: state.VersionSubpathKey(s, 2), duration: writeTime(i, 1), val: senderNonces[s]})

		for j := 0; j < readsPerT-1; j++ {
			ops = append(ops, Op{opType: readType})
		}

		for j := 0; j < nonIOPerT; j++ {
			ops = append(ops, Op{opType: otherType})
		}

		for j := 0; j < writesPerT-1; j++ {
			ops = append(ops, Op{opType: writeType})
		}

		// shuffle ops except for the first three (read nonce, write nonce, another read) ops and last write op.
		// This enables random path generator to generate deterministic paths for these "special" ops.
		for j := 3; j < len(ops)-1; j++ {
			k := rand.Intn(len(ops)-j-1) + j
			ops[j], ops[k] = ops[k], ops[j]
		}

		// Generate time and key path for each op except first two that are always read and write nonce
		for j := 2; j < len(ops); j++ {
			if ops[j].opType == readType {
				ops[j].key = pathGenerator(s, i, j, len(ops))
				ops[j].duration = readTime(i, j)
			} else if ops[j].opType == writeType {
				ops[j].key = pathGenerator(s, i, j, len(ops))
				ops[j].duration = writeTime(i, j)
			} else {
				ops[j].duration = nonIOTime(i, j)
			}

			serialDuration += ops[j].duration
		}

		if ops[len(ops)-1].opType != writeType {
			panic("Last op must be a write")
		}

		t := NewTestExecTask(i, ops, s, senderNonces[s]-1)
		exec = append(exec, t)
	}

	return exec, serialDuration
}

func testExecutorComb(t *testing.T, totalTxs []int, numReads []int, numWrites []int, numNonIO []int, taskRunner TaskRunner) {
	t.Helper()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat())))

	improved := 0
	total := 0

	totalExecDuration := time.Duration(0)
	totalSerialDuration := time.Duration(0)

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					log.Info("Executing block", "numTx", numTx, "numRead", numRead, "numWrite", numWrite, "numNonIO", numNonIO)
					execDuration, expectedSerialDuration := taskRunner(numTx, numRead, numWrite, numNonIO)

					if execDuration < expectedSerialDuration {
						improved++
					}
					total++

					performance := greenTick

					if execDuration >= expectedSerialDuration {
						performance = redCross
					}

					fmt.Printf("exec duration %v, serial duration %v, time reduced %v %.2f%%, %v \n", execDuration, expectedSerialDuration, expectedSerialDuration-execDuration, float64(expectedSerialDuration-execDuration)/float64(expectedSerialDuration)*100, performance)

					totalExecDuration += execDuration
					totalSerialDuration += expectedSerialDuration
				}
			}
		}
	}

	fmt.Println("Improved: ", improved, "Total: ", total, "success rate: ", float64(improved)/float64(total)*100)
	fmt.Printf("Total exec duration: %v, total serial duration: %v, time reduced: %v, time reduced percent: %.2f%%\n", totalExecDuration, totalSerialDuration, totalSerialDuration-totalExecDuration, float64(totalSerialDuration-totalExecDuration)/float64(totalSerialDuration)*100)
}

// nolint: gocognit
func testExecutorCombWithMetadata(t *testing.T, totalTxs []int, numReads []int, numWrites []int, numNonIOs []int, taskRunner TaskRunnerWithMetadata) {
	t.Helper()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat())))

	improved := 0
	improvedMetadata := 0
	rocket := 0
	total := 0

	totalExecDuration := time.Duration(0)
	totalExecDurationMetadata := time.Duration(0)
	totalSerialDuration := time.Duration(0)

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIOs {
					log.Info("Executing block", "numTx", numTx, "numRead", numRead, "numWrite", numWrite, "numNonIO", numNonIO)
					execDuration, execDurationMetadata, expectedSerialDuration := taskRunner(numTx, numRead, numWrite, numNonIO)

					if execDuration < expectedSerialDuration {
						improved++
					}
					total++

					performance := greenTick

					if execDuration >= expectedSerialDuration {
						performance = redCross

						if execDurationMetadata <= expectedSerialDuration {
							performance = threeRockets
							rocket++
						}
					}

					if execDuration >= execDurationMetadata {
						improvedMetadata++
					}

					fmt.Printf("WITHOUT METADATA: exec duration %v, serial duration             %v, time reduced %v %.2f%%, %v \n", execDuration, expectedSerialDuration, expectedSerialDuration-execDuration, float64(expectedSerialDuration-execDuration)/float64(expectedSerialDuration)*100, performance)
					fmt.Printf("WITH METADATA:    exec duration %v, exec duration with metadata %v, time reduced %v %.2f%%\n", execDuration, execDurationMetadata, execDuration-execDurationMetadata, float64(execDuration-execDurationMetadata)/float64(execDuration)*100)

					totalExecDuration += execDuration
					totalExecDurationMetadata += execDurationMetadata
					totalSerialDuration += expectedSerialDuration
				}
			}
		}
	}

	fmt.Println("\nImproved: ", improved, "Total: ", total, "success rate: ", float64(improved)/float64(total)*100)
	fmt.Println("Metadata Better: ", improvedMetadata, "out of: ", total, "success rate: ", float64(improvedMetadata)/float64(total)*100)
	fmt.Println("Rockets (Time of: metadata < serial < without metadata): ", rocket)
	fmt.Printf("\nWithout metadata <> serial:        Total exec duration:          %v, total serial duration       : %v, time reduced: %v, time reduced percent: %.2f%%\n", totalExecDuration, totalSerialDuration, totalSerialDuration-totalExecDuration, float64(totalSerialDuration-totalExecDuration)/float64(totalSerialDuration)*100)
	fmt.Printf("With metadata    <> serial:        Total exec duration metadata: %v, total serial duration       : %v, time reduced: %v, time reduced percent: %.2f%%\n", totalExecDurationMetadata, totalSerialDuration, totalSerialDuration-totalExecDurationMetadata, float64(totalSerialDuration-totalExecDurationMetadata)/float64(totalSerialDuration)*100)
	fmt.Printf("Without metadata <> with metadata: Total exec duration:          %v, total exec duration metadata: %v, time reduced: %v, time reduced percent: %.2f%%\n", totalExecDuration, totalExecDurationMetadata, totalExecDuration-totalExecDurationMetadata, float64(totalExecDuration-totalExecDurationMetadata)/float64(totalExecDuration)*100)
}

func composeValidations(checks []propertyCheck) propertyCheck {
	return func(pe *parallelExecutor) error {
		for _, check := range checks {
			err := check(pe)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func checkNoStatusOverlap(pe *parallelExecutor) error {
	seen := make(map[int]string)

	for blockNum, blockStatus := range pe.blockStatus {
		for _, tx := range blockStatus.execTasks.complete {
			seen[tx] = "complete"
		}

		for _, tx := range blockStatus.execTasks.inProgress {
			if v, ok := seen[tx]; ok {
				return fmt.Errorf("blk %d, tx %v is in both %v and inProgress", blockNum, v, tx)
			}

			seen[tx] = "inProgress"
		}

		for _, tx := range blockStatus.execTasks.pending {
			if v, ok := seen[tx]; ok {
				return fmt.Errorf("blk %d, tx %v is in both %v complete and pending", blockNum, v, tx)
			}

			seen[tx] = "pending"
		}
	}

	return nil
}

func checkNoDroppedTx(pe *parallelExecutor) error {
	for blockNum, blockStatus := range pe.blockStatus {
		for i := 0; i < len(blockStatus.tasks); i++ {
			if !blockStatus.execTasks.checkComplete(i) && !blockStatus.execTasks.checkInProgress(i) && !blockStatus.execTasks.checkPending(i) {
				if !blockStatus.execTasks.isBlocked(i) {
					return fmt.Errorf("blk %d, tx %v is not in any status and is not blocked by any other tx", blockNum, i)
				}
			}
		}
	}

	return nil
}

// nolint: unparam
func runParallel(t *testing.T, tasks []exec.Task, validation propertyCheck, metadata bool) time.Duration {
	t.Helper()

	profile := false

	start := time.Now()
	result, err := executeParallelWithCheck(t, tasks, false, validation, metadata, log.Root())

	if result.Deps != nil && profile {
		Report(result.Deps, *result.Stats, func(str string) { fmt.Println(str) })
	}

	assert.NoError(t, err, "error occur during parallel execution")

	// Need to apply the final write set to storage

	finalWriteSet := make(map[state.VersionKey]time.Duration)

	for _, task := range tasks {
		task := task.(*testExecTask)
		for _, op := range task.ops {
			if op.opType == writeType {
				finalWriteSet[op.key] = op.duration
			}
		}
	}

	for _, v := range finalWriteSet {
		sleep(v)
	}

	duration := time.Since(start)

	return duration
}

type propertyCheck func(*parallelExecutor) error

func executeParallelWithCheck(t *testing.T, tasks []exec.Task, profile bool, check propertyCheck, metadata bool, logger log.Logger) (result blockResult, err error) {
	if len(tasks) == 0 {
		return blockResult{}, nil
	}

	rawDb := memdb.NewStateDB("")
	defer rawDb.Close()

	agg, err := statelib.NewAggregator(context.Background(), datadir.New(""), 16, rawDb, log.New())
	assert.NoError(t, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	assert.NoError(t, err)

	tx, err := db.BeginTemporalRo(context.Background()) //nolint:gocritic
	assert.NoError(t, err)
	defer tx.Rollback()

	domains, err := statelib.NewSharedDomains(tx, log.New())
	assert.NoError(t, err)
	defer domains.Close()

	domains.SetTxNum(1)
	domains.SetBlockNum(1)
	assert.NoError(t, err)

	pe := &parallelExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: params.MainnetChainConfig,
				db:          db,
			},
			doms:           domains,
			rs:             state.NewStateV3(domains, logger),
			outputTxNum:    &atomic.Uint64{},
			outputBlockNum: stages.SyncMetrics[stages.Execution],
			logger:         logger,
		},
		logEvery:    time.NewTicker(20 * time.Second),
		pruneEvery:  time.NewTicker(2 * time.Second),
		workerCount: runtime.NumCPU() - 1,
	}

	executorCancel := pe.run(context.Background(), 1+uint64(len(tasks)), logger)
	defer executorCancel()

	_, err = pe.execute(context.Background(), tasks)

	if err != nil {
		return
	}

	pe.wait(context.Background())

	if check != nil {
		err = check(pe)
	}

	return
}

func runParallelGetMetadata(t *testing.T, tasks []exec.Task, validation propertyCheck) map[int]map[int]bool {
	t.Helper()

	res, err := executeParallelWithCheck(t, tasks, true, validation, false, log.Root())

	assert.NoError(t, err, "error occur during parallel execution")

	return res.AllDeps
}

func TestLessConflicts(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address {
			randomness := rand.Intn(10) + 10
			return common.BigToAddress(big.NewInt(int64(i % randomness)))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestLessConflictsWithMetadata(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIOs := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) common.Address {
			randomness := rand.Intn(10) + 10
			return common.BigToAddress(big.NewInt(int64(i % randomness)))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false)

		allDeps := runParallelGetMetadata(t, tasks, checks)

		newTasks := make([]exec.Task, 0, len(tasks))

		for _, t := range tasks {
			temp := t.(*testExecTask)

			keys := make([]int, len(allDeps[temp.Version().TxIndex]))

			i := 0

			for k := range allDeps[temp.Version().TxIndex] {
				keys[i] = k
				i++
			}

			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}

		return parallelDuration, runParallel(t, newTasks, checks, true), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIOs, taskRunner)
}

func TestZeroTx(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{0}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(1))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestAlternatingTx(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(i % 2))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestAlternatingTxWithMetadata(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(i % 2))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false)

		allDeps := runParallelGetMetadata(t, tasks, checks)

		newTasks := make([]exec.Task, 0, len(tasks))

		for _, t := range tasks {
			temp := t.(*testExecTask)

			keys := make([]int, len(allDeps[temp.Version().TxIndex]))

			i := 0

			for k := range allDeps[temp.Version().TxIndex] {
				keys[i] = k
				i++
			}

			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}

		return parallelDuration, runParallel(t, newTasks, checks, true), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestMoreConflicts(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address {
			randomness := rand.Intn(10) + 10
			return common.BigToAddress(big.NewInt(int64(i / randomness)))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestMoreConflictsWithMetadata(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) common.Address {
			randomness := rand.Intn(10) + 10
			return common.BigToAddress(big.NewInt(int64(i / randomness)))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false)

		allDeps := runParallelGetMetadata(t, tasks, checks)

		newTasks := make([]exec.Task, 0, len(tasks))

		for _, t := range tasks {
			temp := t.(*testExecTask)

			keys := make([]int, len(allDeps[temp.Version().TxIndex]))

			i := 0

			for k := range allDeps[temp.Version().TxIndex] {
				keys[i] = k
				i++
			}

			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}

		return parallelDuration, runParallel(t, newTasks, checks, true), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestRandomTx(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		// Randomly assign this tx to one of 10 senders
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(rand.Intn(10)))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestRandomTxWithMetadata(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		// Randomly assign this tx to one of 10 senders
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(rand.Intn(10)))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false)

		allDeps := runParallelGetMetadata(t, tasks, checks)

		newTasks := make([]exec.Task, 0, len(tasks))

		for _, t := range tasks {
			temp := t.(*testExecTask)

			keys := make([]int, len(allDeps[temp.Version().TxIndex]))

			i := 0

			for k := range allDeps[temp.Version().TxIndex] {
				keys[i] = k
				i++
			}

			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}

		return parallelDuration, runParallel(t, newTasks, checks, true), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestTxWithLongTailRead(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address {
			randomness := rand.Intn(10) + 10
			return common.BigToAddress(big.NewInt(int64(i / randomness)))
		}

		longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)

		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestTxWithLongTailReadWithMetadata(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) common.Address {
			randomness := rand.Intn(10) + 10
			return common.BigToAddress(big.NewInt(int64(i / randomness)))
		}

		longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)

		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false)

		allDeps := runParallelGetMetadata(t, tasks, checks)

		newTasks := make([]exec.Task, 0, len(tasks))

		for _, t := range tasks {
			temp := t.(*testExecTask)

			keys := make([]int, len(allDeps[temp.Version().TxIndex]))

			i := 0

			for k := range allDeps[temp.Version().TxIndex] {
				keys[i] = k
				i++
			}

			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}

		return parallelDuration, runParallel(t, newTasks, checks, true), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestDexScenario(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	postValidation := func(pe *parallelExecutor) error {
		for blockNum, blockStatus := range pe.blockStatus {
			if blockStatus.validateTasks.maxAllComplete() == len(blockStatus.tasks) {
				for i, inputs := range blockStatus.result.TxIO.Inputs() {
					for _, input := range inputs {
						if input.V.TxIndex != i-1 {
							return fmt.Errorf("Blk %d, Tx %d should depend on tx %d, but it actually depends on %d", blockNum, i, i-1, input.V.TxIndex)
						}
					}
				}
			}
		}

		return nil
	}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, postValidation, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(i))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, dexPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}

func TestDexScenarioWithMetadata(t *testing.T) {
	t.Parallel()
	rand.Seed(0)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	postValidation := func(pe *parallelExecutor) error {
		for blockNum, blockStatus := range pe.blockStatus {
			if blockStatus.validateTasks.maxAllComplete() == len(blockStatus.tasks) {
				for i, inputs := range blockStatus.result.TxIO.Inputs() {
					for _, input := range inputs {
						if input.V.TxIndex != i-1 {
							return fmt.Errorf("Blk %d, Tx %d should depend on tx %d, but it actually depends on %d", blockNum, i, i-1, input.V.TxIndex)
						}
					}
				}
			}
		}

		return nil
	}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, postValidation, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(i))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, dexPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false)

		allDeps := runParallelGetMetadata(t, tasks, checks)

		newTasks := make([]exec.Task, 0, len(tasks))

		for _, t := range tasks {
			temp := t.(*testExecTask)

			keys := make([]int, len(allDeps[temp.Version().TxIndex]))

			i := 0

			for k := range allDeps[temp.Version().TxIndex] {
				keys[i] = k
				i++
			}

			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}

		return parallelDuration, runParallel(t, newTasks, checks, true), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner)
}
