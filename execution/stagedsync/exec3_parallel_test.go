// these tests have cleanup issues for mdbx on windows
package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
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
	ctx          context.Context
	ops          []Op
	readMap      state.ReadSet
	writeMap     state.WriteSet
	sender       accounts.Address
	nonce        int
	dependencies []int
}

type PathGenerator func(i int, j int, total int) opkey

type TaskRunner func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration)

type TaskRunnerWithMetadata func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration)

type Timer func(txIdx int, opIdx int) time.Duration

type Sender func(int) accounts.Address

func NewTestExecTask(txIdx int, ops []Op, sender accounts.Address, nonce int) *testExecTask {

	return &testExecTask{
		TxTask: &exec.TxTask{
			Header: &types.Header{
				Number: *uint256.NewInt(1),
			},
			TxNum:   1 + uint64(txIdx),
			TxIndex: txIdx,
		},
		ctx:          context.Background(),
		ops:          ops,
		readMap:      state.ReadSet{},
		writeMap:     state.WriteSet{},
		sender:       sender,
		nonce:        nonce,
		dependencies: []int{},
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	time.Sleep(d)
	return nil
}

func (t *testExecTask) Execute(evm *vm.EVM,
	engine rules.Engine,
	genesis *types.Genesis,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	chainConfig *chain.Config,
	chainReader rules.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) *exec.TxResult {
	// Sleep for 50 microsecond to simulate setup time
	sleepWithContext(t.ctx, time.Microsecond*50) //nolint:errcheck

	version := t.Version()

	t.readMap = state.ReadSet{}
	t.writeMap = state.WriteSet{}

	dep := -1

	for i, op := range t.ops {
		k := op.key

		switch op.opType {
		case readType:
			if _, ok := t.writeMap[k.addr][state.AccountKey{Path: k.path, Key: k.key}]; ok {
				sleepWithContext(t.ctx, op.duration) //nolint:errcheck
				continue
			}

			result := ibs.ReadVersion(k.addr, k.path, k.key, version.TxIndex)

			val := result.Value()

			if i == 0 && val != nil && (val.(int) != t.nonce) {
				return &exec.TxResult{Err: protocol.ErrExecAbortError{
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

			sleepWithContext(t.ctx, op.duration) //nolint:errcheck

			t.readMap.Set(state.VersionedRead{Address: k.addr, Path: k.path, Key: k.key, Source: readKind, Version: state.Version{TxIndex: result.DepIdx(), Incarnation: result.Incarnation()}})
		case writeType:
			t.writeMap.Set(state.VersionedWrite{Address: k.addr, Path: k.path, Key: k.key, Version: version, Val: op.val})
		case otherType:
			sleepWithContext(t.ctx, op.duration) //nolint:errcheck
		default:
			panic(fmt.Sprintf("Unknown op type: %d", op.opType))
		}
	}

	if dep != -1 {
		return &exec.TxResult{Err: protocol.ErrExecAbortError{DependencyTxIndex: dep, OriginError: fmt.Errorf("Dependency error")}}
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

func (t *testExecTask) Sender() accounts.Address {
	return t.sender
}

func (t *testExecTask) Hash() common.Hash {
	return common.BytesToHash(fmt.Appendf(nil, "%d", t.TxIndex))
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
	addr accounts.Address
	key  accounts.StorageKey
	path state.AccountPath
}

var randomPathGenerator = func(i int, j int, total int) opkey {
	addr := accounts.InternAddress(common.BigToAddress((big.NewInt(int64(i % 10)))))
	hash := accounts.InternKey(common.BigToHash((big.NewInt(int64(total)))))
	return opkey{addr, hash, state.StoragePath}
}

var dexPathGenerator = func(i int, j int, total int) opkey {
	if j == total-1 || j == 2 {
		addr := accounts.InternAddress(common.BigToAddress(big.NewInt(int64(0))))
		return opkey{addr: addr, path: state.BalancePath}
	} else {
		addr := accounts.InternAddress(common.BigToAddress(big.NewInt(int64(j))))
		return opkey{addr: addr, path: state.BalancePath}
	}
}

var readTime = randTimeGenerator(4*time.Microsecond, 12*time.Microsecond)
var writeTime = randTimeGenerator(2*time.Microsecond, 6*time.Microsecond)
var nonIOTime = randTimeGenerator(1*time.Microsecond, 2*time.Microsecond)

func taskFactory(numTask int, sender Sender, readsPerT int, writesPerT int, nonIOPerT int, pathGenerator PathGenerator, readTime Timer, writeTime Timer, nonIOTime Timer) ([]exec.Task, time.Duration) {
	exec := make([]exec.Task, 0, numTask)

	var serialDuration time.Duration

	senderNonces := make(map[accounts.Address]int)

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
				ops[j].key = pathGenerator(i, j, len(ops))
				ops[j].duration = readTime(i, j)
			} else if ops[j].opType == writeType {
				ops[j].key = pathGenerator(i, j, len(ops))
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

func testExecutorComb(tb testing.TB, totalTxs []int, numReads []int, numWrites []int, numNonIO []int, taskRunner TaskRunner, logger log.Logger) {
	tb.Helper()

	improved := 0
	total := 0

	totalExecDuration := time.Duration(0)
	totalSerialDuration := time.Duration(0)

	fmt.Println("\n" + tb.Name())
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
func testExecutorCombWithMetadata(tb testing.TB, totalTxs []int, numReads []int, numWrites []int, numNonIOs []int, taskRunner TaskRunnerWithMetadata, logger log.Logger) {
	tb.Helper()

	improved := 0
	improvedMetadata := 0
	rocket := 0
	total := 0

	totalExecDuration := time.Duration(0)
	totalExecDurationMetadata := time.Duration(0)
	totalSerialDuration := time.Duration(0)
	fmt.Println("\n" + tb.Name())
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

	pe.RLock()
	defer pe.RUnlock()

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
	pe.RLock()
	defer pe.RUnlock()

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

func runParallel(tb testing.TB, tasks []exec.Task, validation propertyCheck, metadata bool, logger log.Logger) time.Duration {
	tb.Helper()

	tmpDir, err := os.MkdirTemp("", "erigon-parallel-test-*")
	if err != nil {
		tb.Fatal(err)
	}
	defer dir.RemoveAll(tmpDir)

	dirs := datadir.New(tmpDir)
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).MustOpen()

	defer rawDb.Close()

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(logger).Open(context.Background(), rawDb)
	assert.NoError(tb, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	assert.NoError(tb, err)

	tx, err := db.BeginTemporalRo(context.Background()) //nolint:gocritic
	assert.NoError(tb, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	assert.NoError(tb, err)
	defer domains.Close()

	assert.NoError(tb, err)

	chainSpec, _ := chainspec.ChainSpecByName(networkname.Mainnet)

	pe := &parallelExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: chainSpec.Config,
				db:          db,
			},
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, logger)),
			logger: logger,
		},
		workerCount: runtime.NumCPU() - 1,
	}

	executorContext, executorCancel, err := pe.run(context.Background())

	assert.NoError(tb, err, "error occur during parallel init")
	assert.NoError(tb, executorContext.Err(), "error occur during parallel init")

	defer executorCancel()

	for _, task := range tasks {
		task := task.(*testExecTask)
		task.TxTask.Config = chainSpec.Config
		task.ctx = executorContext //nolint:fatcontext
	}

	start := time.Now()
	_, err = executeParallelWithCheck(tb, pe, tasks, false, validation, metadata)

	assert.NoError(tb, err, "error occur during parallel execution")

	// Need to apply the final write set to storage

	finalWriteSet := map[accounts.Address]map[state.AccountKey]time.Duration{}

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
			sleepWithContext(executorContext, d) //nolint:errcheck
		}
	}

	duration := time.Since(start)

	return duration
}

type propertyCheck func(*parallelExecutor) error

func executeParallelWithCheck(tb testing.TB, pe *parallelExecutor, tasks []exec.Task, profile bool, check propertyCheck, metadata bool) (result *blockResult, err error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	applyResults := make(chan applyResult, 1000)

	pe.execRequests <- &execRequest{0, common.Hash{}, nil, nil, tasks, applyResults, profile, nil}

	// TODO get results back

	for applyResult := range applyResults {
		var ok bool
		if result, ok = applyResult.(*blockResult); ok {
			break
		}
	}

	cancel()
	pe.wait(ctx)

	if check != nil {
		err = check(pe)
	}

	return result, err
}

func runParallelGetMetadata(tb testing.TB, tasks []exec.Task, validation propertyCheck) map[int]map[int]bool {
	tb.Helper()

	logger := log.Root()

	tmpDir, err := os.MkdirTemp("", "erigon-parallel-meta-*")
	if err != nil {
		tb.Fatal(err)
	}
	defer dir.RemoveAll(tmpDir)

	dirs := datadir.New(tmpDir)
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).MustOpen()
	defer rawDb.Close()
	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(logger).Open(context.Background(), rawDb)
	assert.NoError(tb, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	assert.NoError(tb, err)

	tx, err := db.BeginTemporalRo(context.Background()) //nolint:gocritic
	assert.NoError(tb, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	assert.NoError(tb, err)
	defer domains.Close()

	chainSpec, _ := chainspec.ChainSpecByName(networkname.Mainnet)

	pe := &parallelExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: chainSpec.Config,
				db:          db,
			},
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, logger)),
			logger: logger,
		},
		workerCount: runtime.NumCPU() - 1,
	}

	executorContext, executorCancel, err := pe.run(context.Background())
	defer executorCancel()
	assert.NoError(tb, err, "error occur during parallel init")

	for _, task := range tasks {
		task := task.(*testExecTask)
		task.ctx = executorContext //nolint:fatcontext
	}

	res, err := executeParallelWithCheck(tb, pe, tasks, true, validation, false)

	assert.NoError(tb, err, "error occur during parallel execution")

	return res.AllDeps
}

// runProfileAndExecute profiles tasks to discover dependencies, then re-runs with that metadata.
// Uses a single DB stack for both passes, halving the MDBX/aggregator/temporal setup cost.
func runProfileAndExecute(tb testing.TB, tasks []exec.Task, validation propertyCheck, logger log.Logger) time.Duration {
	tb.Helper()

	tmpDir, err := os.MkdirTemp("", "erigon-parallel-meta-test-*")
	if err != nil {
		tb.Fatal(err)
	}
	defer dir.RemoveAll(tmpDir)

	dirs := datadir.New(tmpDir)
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).MustOpen()
	defer rawDb.Close()

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(logger).Open(context.Background(), rawDb)
	assert.NoError(tb, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	assert.NoError(tb, err)

	chainSpec, _ := chainspec.ChainSpecByName(networkname.Mainnet)

	// newExecutor creates a fresh domains/state/executor on the shared DB.
	newExecutor := func() (*parallelExecutor, context.Context, context.CancelFunc, func()) {
		tx, err := db.BeginTemporalRo(context.Background()) //nolint:gocritic
		assert.NoError(tb, err)
		domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
		assert.NoError(tb, err)

		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:    ExecuteBlockCfg{chainConfig: chainSpec.Config, db: db},
				doms:   domains,
				rs:     state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, logger)),
				logger: logger,
			},
			workerCount: runtime.NumCPU() - 1,
		}

		executorCtx, executorCancel, err := pe.run(context.Background())
		assert.NoError(tb, err, "error during parallel init")

		cleanup := func() {
			executorCancel()
			domains.Close()
			tx.Rollback()
		}
		return pe, executorCtx, executorCancel, cleanup
	}

	// Pass 1: profile to discover dependency metadata
	var allDeps map[int]map[int]bool
	{
		pe, executorCtx, _, cleanup := newExecutor()
		defer cleanup()

		for _, task := range tasks {
			task := task.(*testExecTask)
			task.TxTask.Config = chainSpec.Config
			task.ctx = executorCtx //nolint:fatcontext
		}

		res, err := executeParallelWithCheck(tb, pe, tasks, true, validation, false)
		assert.NoError(tb, err, "error during profiling pass")
		cleanup() // cleanup eagerly so pass 2 gets a clean slate
		allDeps = res.AllDeps
	}

	// Apply dependency metadata
	tasks = applyDeps(tasks, allDeps)

	// Pass 2: execute with metadata
	pe, executorCtx, _, cleanup := newExecutor()
	defer cleanup()

	for _, task := range tasks {
		task := task.(*testExecTask)
		task.TxTask.Config = chainSpec.Config
		task.ctx = executorCtx //nolint:fatcontext
	}

	start := time.Now()
	_, err = executeParallelWithCheck(tb, pe, tasks, false, validation, true)
	assert.NoError(tb, err, "error during metadata execution pass")

	finalWriteSet := map[accounts.Address]map[state.AccountKey]time.Duration{}
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
			sleepWithContext(executorCtx, d) //nolint:errcheck
		}
	}

	return time.Since(start)
}

var discardLogging = true

// lessConflictsSender returns a sender function that distributes txs across many addresses (low contention).
func lessConflictsSender(rng *rand.Rand) Sender {
	return func(i int) accounts.Address {
		randomness := rng.Intn(10) + 10
		return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % randomness))))
	}
}

// moreConflictsSender returns a sender function that clusters txs onto fewer addresses (high contention).
func moreConflictsSender(rng *rand.Rand) Sender {
	return func(i int) accounts.Address {
		randomness := rng.Intn(10) + 10
		return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i / randomness))))
	}
}

// randomSender returns a sender function that randomly assigns txs to one of 10 addresses.
func randomSender(rng *rand.Rand) Sender {
	return func(i int) accounts.Address {
		return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(rng.Intn(10)))))
	}
}

// applyDeps enriches tasks with dependency metadata from a prior profiling run.
func applyDeps(tasks []exec.Task, allDeps map[int]map[int]bool) []exec.Task {
	newTasks := make([]exec.Task, 0, len(tasks))
	for _, task := range tasks {
		temp := task.(*testExecTask)
		keys := make([]int, 0, len(allDeps[temp.Version().TxIndex]))
		for k := range allDeps[temp.Version().TxIndex] {
			keys = append(keys, k)
		}
		temp.dependencies = keys
		newTasks = append(newTasks, temp)
	}
	return newTasks
}

var defaultChecks = composeValidations([]propertyCheck{checkNoStatusOverlap, checkNoDroppedTx})

// --- Simple correctness tests (single run, small params) ---

// TestZeroTx verifies the parallel executor handles an empty transaction list.
func TestZeroTx(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	sender := func(i int) accounts.Address { return accounts.InternAddress(common.BigToAddress(big.NewInt(1))) }
	tasks, _ := taskFactory(0, sender, 5, 5, 10, randomPathGenerator, readTime, writeTime, nonIOTime)
	runParallel(t, tasks, defaultChecks, false, log.New())
}

// TestLessConflicts verifies correctness with low-contention sender distribution.
// Use BenchmarkLessConflicts for the full parameter sweep.
func TestLessConflicts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	rng := rand.New(rand.NewSource(0))
	tasks, _ := taskFactory(10, lessConflictsSender(rng), 5, 5, 10, randomPathGenerator, readTime, writeTime, nonIOTime)
	runParallel(t, tasks, defaultChecks, false, log.New())
}

// TestMoreConflicts verifies correctness with high-contention sender distribution.
// Use BenchmarkMoreConflicts for the full parameter sweep.
func TestMoreConflicts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	rng := rand.New(rand.NewSource(0))
	tasks, _ := taskFactory(10, moreConflictsSender(rng), 5, 5, 10, randomPathGenerator, readTime, writeTime, nonIOTime)
	runParallel(t, tasks, defaultChecks, false, log.New())
}

// TestRandomTx verifies correctness with fully random sender assignment.
// Use BenchmarkRandomTx for the full parameter sweep.
func TestRandomTx(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	rng := rand.New(rand.NewSource(0))
	tasks, _ := taskFactory(10, randomSender(rng), 5, 5, 10, randomPathGenerator, readTime, writeTime, nonIOTime)
	runParallel(t, tasks, defaultChecks, false, log.New())
}

// TestAlternatingTx verifies correctness when txs alternate between two senders.
func TestAlternatingTx(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	sender := func(i int) accounts.Address {
		return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % 2))))
	}
	tasks, _ := taskFactory(10, sender, 5, 5, 10, randomPathGenerator, readTime, writeTime, nonIOTime)
	runParallel(t, tasks, defaultChecks, false, log.New())
}

// TestTxWithLongTailRead verifies correctness when occasional reads have 100x latency spikes.
// Use BenchmarkTxWithLongTailRead for the full parameter sweep.
func TestTxWithLongTailRead(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	rng := rand.New(rand.NewSource(0))
	longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)
	tasks, _ := taskFactory(10, moreConflictsSender(rng), 5, 5, 10, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)
	runParallel(t, tasks, defaultChecks, false, log.New())
}

// --- Simple correctness tests with metadata ---

// TestAlternatingTxWithMetadata verifies correctness with pre-computed dependency metadata
// when txs alternate between two senders.
func TestAlternatingTxWithMetadata(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	sender := func(i int) accounts.Address {
		return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % 2))))
	}
	tasks, _ := taskFactory(10, sender, 5, 5, 10, randomPathGenerator, readTime, writeTime, nonIOTime)
	runProfileAndExecute(t, tasks, defaultChecks, log.New())
}

// --- Benchmarks (full parameter sweeps, only run with -bench) ---

// BenchmarkLessConflicts runs the full low-contention parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkLessConflicts -benchtime=1x
func BenchmarkLessConflicts(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						tasks, serialDuration := taskFactory(numTx, lessConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkLessConflictsWithMetadata runs the low-contention parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkLessConflictsWithMetadata -benchtime=1x
func BenchmarkLessConflictsWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		tasks, serialDuration := taskFactory(numTx, lessConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkMoreConflicts runs the full high-contention parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkMoreConflicts -benchtime=1x
func BenchmarkMoreConflicts(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkMoreConflictsWithMetadata runs the high-contention parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkMoreConflictsWithMetadata -benchtime=1x
func BenchmarkMoreConflictsWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkRandomTx runs the full random-sender parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkRandomTx -benchtime=1x
func BenchmarkRandomTx(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						tasks, serialDuration := taskFactory(numTx, randomSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkRandomTxWithMetadata runs the random-sender parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkRandomTxWithMetadata -benchtime=1x
func BenchmarkRandomTxWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		tasks, serialDuration := taskFactory(numTx, randomSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkTxWithLongTailRead runs the full parameter sweep with occasional 100x read latency spikes.
// Run with: go test -run='^$' -bench=BenchmarkTxWithLongTailRead -benchtime=1x
func BenchmarkTxWithLongTailRead(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(0))
						longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)
						tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// BenchmarkTxWithLongTailReadWithMetadata runs the long-tail-read parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkTxWithLongTailReadWithMetadata -benchtime=1x
func BenchmarkTxWithLongTailReadWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		rng := rand.New(rand.NewSource(0))
		longTailReadTimer := longTailTimeGenerator(4*time.Microsecond, 12*time.Microsecond, 7, 10)
		tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, longTailReadTimer, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkAlternatingTx runs the alternating-sender parameter sweep.
// Run with: go test -run='^$' -bench=BenchmarkAlternatingTx -benchtime=1x
func BenchmarkAlternatingTx(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration) {
		sender := func(i int) accounts.Address {
			return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % 2))))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		return runParallel(b, tasks, defaultChecks, false, logger), serialDuration
	}

	testExecutorComb(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// BenchmarkAlternatingTxWithMetadata runs the alternating-sender parameter sweep with dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkAlternatingTxWithMetadata -benchtime=1x
func BenchmarkAlternatingTxWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	totalTxs := []int{200}
	numReads := []int{20}
	numWrites := []int{20}
	numNonIO := []int{100}

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) accounts.Address {
			return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i % 2))))
		}
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
		parallelDuration := runParallel(b, tasks, defaultChecks, false, logger)
		allDeps := runParallelGetMetadata(b, tasks, defaultChecks)
		return parallelDuration, runParallel(b, applyDeps(tasks, allDeps), defaultChecks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}

// dexPostValidation checks that each tx correctly depends on the immediately preceding writer.
func dexPostValidation(pe *parallelExecutor) error {
	pe.RLock()
	defer pe.RUnlock()
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

// TestDexScenario verifies correctness of the parallel executor under a DEX-like access pattern
// (many transactions contending on a shared "hub" address).
// Use BenchmarkDexScenario for the full parameter sweep.
func TestDexScenario(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	sender := func(i int) accounts.Address { return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i)))) }
	tasks, _ := taskFactory(10, sender, 5, 5, 10, dexPathGenerator, readTime, writeTime, nonIOTime)
	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, dexPostValidation, checkNoDroppedTx})
	runParallel(t, tasks, checks, false, log.New())
}

// BenchmarkDexScenario runs the full DEX parameter sweep (5×3×3×2 = 90 combinations) and
// reports parallel speedup over expected serial duration.
// Run with: go test -run='^$' -bench=BenchmarkDexScenario -benchtime=1x
func BenchmarkDexScenario(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)

	totalTxs := []int{10, 50, 100, 200, 300}
	numReads := []int{20, 100, 200}
	numWrites := []int{20, 100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, dexPostValidation, checkNoDroppedTx})

	for _, numTx := range totalTxs {
		for _, numRead := range numReads {
			for _, numWrite := range numWrites {
				for _, numNonIO := range numNonIO {
					numTx, numRead, numWrite, numNonIO := numTx, numRead, numWrite, numNonIO
					name := fmt.Sprintf("txs=%d/reads=%d/writes=%d/nonIO=%d", numTx, numRead, numWrite, numNonIO)
					b.Run(name, func(b *testing.B) {
						sender := func(i int) accounts.Address {
							return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i))))
						}
						tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, dexPathGenerator, readTime, writeTime, nonIOTime)
						b.ResetTimer()
						parallelDuration := runParallel(b, tasks, checks, false, logger)
						if parallelDuration > 0 {
							b.ReportMetric(float64(serialDuration)/float64(parallelDuration), "speedup")
						}
					})
				}
			}
		}
	}
}

// TestDexScenarioWithMetadata verifies correctness of the parallel executor with pre-computed
// dependency metadata under a DEX-like access pattern.
// Use BenchmarkDexScenarioWithMetadata for the full parameter sweep.
func TestDexScenarioWithMetadata(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, dexPostValidation, checkNoDroppedTx})
	sender := func(i int) accounts.Address { return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i)))) }
	tasks, _ := taskFactory(10, sender, 5, 5, 10, dexPathGenerator, readTime, writeTime, nonIOTime)
	runProfileAndExecute(t, tasks, checks, log.New())
}

// BenchmarkDexScenarioWithMetadata runs the full DEX+metadata parameter sweep and reports
// speedup with and without pre-computed dependency metadata.
// Run with: go test -run='^$' -bench=BenchmarkDexScenarioWithMetadata -benchtime=1x
func BenchmarkDexScenarioWithMetadata(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)

	totalTxs := []int{300}
	numReads := []int{100, 200}
	numWrites := []int{100, 200}
	numNonIO := []int{100, 500}

	checks := composeValidations([]propertyCheck{checkNoStatusOverlap, dexPostValidation, checkNoDroppedTx})

	taskRunner := func(numTx int, numRead int, numWrite int, numNonIO int) (time.Duration, time.Duration, time.Duration) {
		sender := func(i int) accounts.Address { return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i)))) }
		tasks, serialDuration := taskFactory(numTx, sender, numRead, numWrite, numNonIO, dexPathGenerator, readTime, writeTime, nonIOTime)

		parallelDuration := runParallel(b, tasks, checks, false, logger)

		allDeps := runParallelGetMetadata(b, tasks, checks)
		newTasks := make([]exec.Task, 0, len(tasks))
		for _, task := range tasks {
			temp := task.(*testExecTask)
			keys := make([]int, 0, len(allDeps[temp.Version().TxIndex]))
			for k := range allDeps[temp.Version().TxIndex] {
				keys = append(keys, k)
			}
			temp.dependencies = keys
			newTasks = append(newTasks, temp)
		}
		return parallelDuration, runParallel(b, newTasks, checks, true, logger), serialDuration
	}

	testExecutorCombWithMetadata(b, totalTxs, numReads, numWrites, numNonIO, taskRunner, logger)
}
