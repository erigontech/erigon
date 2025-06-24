package stagedsync

///go:build integration

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal"
	statelib "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/consensus"
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
	key      opkey
	duration time.Duration
	opType   OpType
	val      int
}

type testExecTask struct {
	*exec.TxTask
	ops          []Op
	readMap      state.ReadSet
	writeMap     state.WriteSet
	sender       common.Address
	nonce        int
	dependencies []int
}

type PathGenerator func(addr common.Address, i int, j int, total int) opkey

type TaskRunner func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration)

type TaskRunnerWithMetadata func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration)

type Timer func(txIdx int, opIdx int) time.Duration

type Sender func(int) common.Address

func NewTestExecTask(txIdx int, ops []Op, sender common.Address, nonce int) *testExecTask {
	return &testExecTask{
		TxTask: &exec.TxTask{
			Header: &types.Header{
				Number: big.NewInt(1),
			},
			TxNum:   1 + uint64(txIdx),
			TxIndex: txIdx,
		},
		ops:          ops,
		readMap:      state.ReadSet{},
		writeMap:     state.WriteSet{},
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
	engine consensus.Engine,
	genesis *types.Genesis,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) *exec.TxResult {
	// Sleep for 50 microsecond to simulate setup time
	sleep(time.Microsecond * 50)

	version := t.Version()

	t.readMap = state.ReadSet{}
	t.writeMap = state.WriteSet{}

	dep := -1

	for i, op := range t.ops {
		k := op.key

		switch op.opType {
		case readType:
			if _, ok := t.writeMap[k.addr][state.AccountKey{Path: k.path, Key: k.key}]; ok {
				sleep(op.duration)
				continue
			}

			result := ibs.ReadVersion(k.addr, k.path, k.key, version.TxIndex)

			val := result.Value()

			if i == 0 && val != nil && (val.(int) != t.nonce) {
				return &exec.TxResult{Err: core.ErrExecAbortError{
					DependencyTxIndex: -1,
					OriginError:       fmt.Errorf("invalid nonce: got: %d, expected: %d", val.(int), t.nonce)}}
			}

			if result.Status() == state.MVReadResultDependency {
				if result.DepIdx() > dep {
					dep = result.DepIdx()
				}
			}

			var readKind state.ReadSource

			if result.Status() == state.MVReadResultDone {
				readKind = state.MapRead
			} else if result.Status() == state.MVReadResultNone {
				readKind = state.StorageRead
			}

			sleep(op.duration)

			t.readMap.Set(state.VersionedRead{Address: k.addr, Path: k.path, Key: k.key, Source: readKind, Version: state.Version{TxIndex: result.DepIdx(), Incarnation: result.Incarnation()}})
		case writeType:
			t.writeMap.Set(state.VersionedWrite{Address: k.addr, Path: k.path, Key: k.key, Version: version, Val: op.val})
		case otherType:
			sleep(op.duration)
		default:
			panic(fmt.Sprintf("Unknown op type: %d", op.opType))
		}
	}

	if dep != -1 {
		return &exec.TxResult{Err: core.ErrExecAbortError{DependencyTxIndex: dep, OriginError: fmt.Errorf("Dependency error")}}
	}

	return &exec.TxResult{}
}

func (t *testExecTask) VersionedWrites(_ *state.IntraBlockState) state.VersionedWrites {
	writes := make(state.VersionedWrites, 0, t.writeMap.Len())

	t.writeMap.Scan(func(v *state.VersionedWrite) bool {
		writes = append(writes, v)
		return true
	})

	return writes
}

func (t *testExecTask) VersionedReads(_ *state.IntraBlockState) state.ReadSet {
	return t.readMap
}

func (t *testExecTask) Sender() common.Address {
	return t.sender
}

func (t *testExecTask) Hash() common.Hash {
	return common.BytesToHash([]byte(fmt.Sprintf("%d", t.TxIndex)))
}

func (t *testExecTask) Dependencies() []int {
	return t.dependencies
}

func logger(discardLogging bool) log.Logger {
	var handler log.Handler

	if discardLogging {
		handler = log.DiscardHandler()
	} else {
		handler = log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat()))
	}

	log.Root().SetHandler(handler)

	return log.Root()
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

type opkey struct {
	addr common.Address
	key  common.Hash
	path state.AccountPath
}

var randomPathGenerator = func(sender common.Address, i int, j int, total int) opkey {
	addr := common.BigToAddress((big.NewInt(int64(i % 10))))
	hash := common.BigToHash((big.NewInt(int64(total))))
	return opkey{addr, hash, state.StatePath}
}

var dexPathGenerator = func(sender common.Address, i int, j int, total int) opkey {
	if j == total-1 || j == 2 {
		addr := common.BigToAddress(big.NewInt(int64(0)))
		return opkey{addr: addr, path: state.BalancePath}
	} else {
		addr := common.BigToAddress(big.NewInt(int64(j)))
		return opkey{addr: addr, path: state.BalancePath}
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

		ops = append(ops, Op{opType: readType, key: opkey{addr: s, path: state.NoncePath}, duration: readTime(i, 0), val: senderNonces[s]})

		senderNonces[s]++

		ops = append(ops, Op{opType: writeType, key: opkey{addr: s, path: state.NoncePath}, duration: writeTime(i, 1), val: senderNonces[s]})

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

func testExecutorComb(t *testing.T, totalTxs []int, numReads []int, numWrites []int, numNonIO []int, taskRunner TaskRunner, logger log.Logger) {
	t.Helper()

	improved := 0
	total := 0

	totalExecDuration := time.Duration(0)
	totalSerialDuration := time.Duration(0)

	fmt.Println("\n" + t.Name())
	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					logger.Info("Executing block", "numTx", numTx, "numRead", numRead, "numWrite", numWrite, "numNonIO", numNonIO)
					execDuration, expectedSerialDuration := taskRunner(numTx, numRead, numWrite, numNonIO)

					if execDuration < expectedSerialDuration {
						improved++
					}
					total++

					performance := greenTick

					if execDuration >= expectedSerialDuration {
						performance = redCross
					}

					fmt.Printf("exec duration %v, serial duration %v, improved %v %.2fx, %v \n", execDuration, expectedSerialDuration, expectedSerialDuration-execDuration, float64(expectedSerialDuration)/float64(execDuration), performance)

					totalExecDuration += execDuration
					totalSerialDuration += expectedSerialDuration
				}
			}
		}
	}

	fmt.Printf("Improved: %v Total: %v success rate: %.2f%%\n", improved, total, float64(improved)/float64(total)*100)
	fmt.Printf("Total exec duration: %v, total serial duration: %v, time reduced: %v, improved: %.2fx\n", totalExecDuration, totalSerialDuration, totalSerialDuration-totalExecDuration, float64(totalSerialDuration)/float64(totalExecDuration))
}

// nolint: gocognit
func testExecutorCombWithMetadata(t *testing.T, totalTxs []int, numReads []int, numWrites []int, numNonIOs []int, taskRunner TaskRunnerWithMetadata, logger log.Logger) {
	t.Helper()

	improved := 0
	improvedMetadata := 0
	rocket := 0
	total := 0

	totalExecDuration := time.Duration(0)
	totalExecDurationMetadata := time.Duration(0)
	totalSerialDuration := time.Duration(0)
	fmt.Println("\n" + t.Name())
	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIOs {
					logger.Info("Executing block", "numTx", numTx, "numRead", numRead, "numWrite", numWrite, "numNonIO", numNonIO)
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

					fmt.Printf("WITHOUT METADATA: exec duration %v, serial duration             %v, improved %v %.2fx, %v \n", execDuration, expectedSerialDuration, expectedSerialDuration-execDuration, float64(expectedSerialDuration)/float64(execDuration), performance)
					fmt.Printf("WITH METADATA:    exec duration %v, exec duration with metadata %v, time reduced %v %.2fx\n", execDuration, execDurationMetadata, execDuration-execDurationMetadata, float64(expectedSerialDuration)/float64(execDurationMetadata))

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
	fmt.Printf("\nWithout metadata <> serial:        Total exec duration:          %v, total serial duration       : %v, time reduced: %v, improved: %.2fx\n", totalExecDuration, totalSerialDuration, totalSerialDuration-totalExecDuration, float64(totalSerialDuration)/float64(totalExecDuration))
	fmt.Printf("With metadata    <> serial:        Total exec duration metadata: %v, total serial duration       : %v, time reduced: %v, improved: %.2fx\n", totalExecDurationMetadata, totalSerialDuration, totalSerialDuration-totalExecDurationMetadata, float64(totalSerialDuration)/float64(totalExecDurationMetadata))
	fmt.Printf("Without metadata <> with metadata: Total exec duration:          %v, total exec duration metadata: %v, time reduced: %v, improved: %.2fx\n", totalExecDuration, totalExecDurationMetadata, totalExecDuration-totalExecDurationMetadata, float64(totalExecDuration)/float64(totalExecDurationMetadata))
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

	for blockNum, blockStatus := range pe.blockExecutors {
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
	for blockNum, blockStatus := range pe.blockExecutors {
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
func runParallel(t *testing.T, tasks []exec.Task, validation propertyCheck, metadata bool, logger log.Logger) time.Duration {
	t.Helper()

	rawDb := memdb.NewStateDB("")
	defer rawDb.Close()

	agg, err := statelib.NewAggregator(context.Background(), datadir.New(""), 16, rawDb, logger)
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
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, logger)),
			logger: logger,
		},
		workerCount: runtime.NumCPU() - 1,
	}

	executorContext, executorCancel := pe.run(context.Background())

	assert.NoError(t, executorContext.Err(), "error occur during parallel init")

	defer executorCancel()

	start := time.Now()
	_, err = executeParallelWithCheck(t, pe, tasks, false, validation, metadata)

	assert.NoError(t, err, "error occur during parallel execution")

	// Need to apply the final write set to storage

	finalWriteSet := map[common.Address]map[state.AccountKey]time.Duration{}

	for _, task := range tasks {
		task := task.(*testExecTask)
		for _, op := range task.ops {
			if op.opType == writeType {
				writes, ok := finalWriteSet[op.key.addr]
				if !ok {
					writes = map[state.AccountKey]time.Duration{}
					finalWriteSet[op.key.addr] = writes
				}
				writes[state.AccountKey{Path: op.key.path, Key: op.key.key}] = op.duration
			}
		}
	}

	for _, writes := range finalWriteSet {
		for _, d := range writes {
			sleep(d)
		}
	}

	duration := time.Since(start)

	return duration
}

type propertyCheck func(*parallelExecutor) error

func executeParallelWithCheck(t *testing.T, pe *parallelExecutor, tasks []exec.Task, profile bool, check propertyCheck, metadata bool) (result *blockResult, err error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	applyResults := make(chan applyResult, 1000)

	pe.execRequests <- &execRequest{0, common.Hash{}, nil, tasks, applyResults, profile}

	defer pe.wait(ctx)

	// TODO get results back

	for applyResult := range applyResults {
		var ok bool
		if result, ok = applyResult.(*blockResult); ok {
			break
		}
	}

	if check != nil {
		err = check(pe)
	}

	cancel()

	return result, err
}

func runParallelGetMetadata(t *testing.T, tasks []exec.Task, validation propertyCheck) map[int]map[int]bool {
	t.Helper()

	logger := log.Root()

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
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, logger)),
			logger: logger,
		},
		workerCount: runtime.NumCPU() - 1,
	}

	_, executorCancel := pe.run(context.Background())

	defer executorCancel()

	res, err := executeParallelWithCheck(t, pe, tasks, true, validation, false)

	assert.NoError(t, err, "error occur during parallel execution")

	return res.AllDeps
}

var discardLogging = true

func TestLessConflicts(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

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

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestLessConflictsWithMetadata(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

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

		parallelDuration := runParallel(t, tasks, checks, false, logger)

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

		return parallelDuration, runParallel(t, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIOs, taskRunner, logger)
}

func TestZeroTx(t *testing.T) {
	//t.Parallel()
	logger := logger(discardLogging)

	totalTxs := []int{0}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(1))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestAlternatingTx(t *testing.T) {
	//t.Parallel()

	logger := logger(discardLogging)

	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(i % 2))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestAlternatingTxWithMetadata(t *testing.T) {
	//t.Parallel()
	logger := logger(discardLogging)

	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(i % 2))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false, logger)

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

		return parallelDuration, runParallel(t, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestMoreConflicts(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

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

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestMoreConflictsWithMetadata(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

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

		parallelDuration := runParallel(t, tasks, checks, false, logger)

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

		return parallelDuration, runParallel(t, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestRandomTx(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		// Randomly assign this tx to one of 10 senders
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(rand.Intn(10)))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestRandomTxWithMetadata(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		// Randomly assign this tx to one of 10 senders
		sender := func(i int) common.Address { return common.BigToAddress(big.NewInt(int64(rand.Intn(10)))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(t, tasks, checks, false, logger)

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

		return parallelDuration, runParallel(t, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestTxWithLongTailRead(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

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

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestTxWithLongTailReadWithMetadata(t *testing.T) {
	//t.Parallel()
	rand := rand.New(rand.NewSource(0))

	logger := logger(discardLogging)

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

		parallelDuration := runParallel(t, tasks, checks, false, logger)

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

		return parallelDuration, runParallel(t, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestDexScenario(t *testing.T) {
	//t.Parallel()
	logger := logger(discardLogging)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	postValidation := func(pe *parallelExecutor) error {
		for blockNum, blockStatus := range pe.blockExecutors {
			if blockStatus.validateTasks.maxComplete() == len(blockStatus.tasks) {
				for i, inputs := range blockStatus.result.TxIO.Inputs() {
					var err error
					inputs.Scan(func(input *state.VersionedRead) bool {
						if input.Version.TxIndex != i-1 {
							err = fmt.Errorf("Blk %d, Tx %d should depend on tx %d, but it actually depends on %d", blockNum, i, i-1, input.Version.TxIndex)
							return false
						}
						return true
					})
					if err != nil {
						return err
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

		return runParallel(t, tasks, checks, false, logger), serialDuration
	}

	testExecutorComb(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

func TestDexScenarioWithMetadata(t *testing.T) {
	//t.Parallel()
	logger := logger(discardLogging)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	postValidation := func(pe *parallelExecutor) error {
		for blockNum, blockStatus := range pe.blockExecutors {
			if blockStatus.validateTasks.maxComplete() == len(blockStatus.tasks) {
				for i, inputs := range blockStatus.result.TxIO.Inputs() {
					var err error
					inputs.Scan(func(input *state.VersionedRead) bool {
						if input.Version.TxIndex != i-1 {
							err = fmt.Errorf("Blk %d, Tx %d should depend on tx %d, but it actually depends on %d", blockNum, i, i-1, input.Version.TxIndex)
							return false
						}
						return true
					})
					if err != nil {
						return err
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

		parallelDuration := runParallel(t, tasks, checks, false, logger)

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

		return parallelDuration, runParallel(t, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(t, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}
