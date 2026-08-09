package stagedsync

import (
	"fmt"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Per-op costs of a plain transfer, sized so the exec-loop's serial work is not
// buried under simulated EVM time — a 21k-gas tx is a few microseconds of real work.
const (
	smallTxSetup     = 2 * time.Microsecond
	smallTxReadTime  = 200 * time.Nanosecond
	smallTxWriteTime = 100 * time.Nanosecond
	hotRecipients    = 8
)

type smallTxShape struct {
	name string
	// erc20 adds a shared token contract with per-participant storage slots.
	erc20 bool
	// hotPct is the share of txs whose recipient comes from a small hot set,
	// modelling exchange/router addresses that serialize an otherwise wide block.
	hotPct int
}

func smallTxAddr(i int) accounts.Address {
	return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i))))
}

func smallTxSlot(i int) accounts.StorageKey {
	return accounts.InternKey(common.BigToHash(big.NewInt(int64(i))))
}

// smallTxTaskFactory builds a block of independent small transfers: every tx has
// its own sender, so any serialization the executor shows is its own, not the
// workload's.
func smallTxTaskFactory(numTx int, shape smallTxShape) []exec.Task {
	tasks := make([]exec.Task, 0, numTx)
	// Senders, recipients and the token contract occupy disjoint address ranges.
	const recipientBase = 1_000_000
	const tokenAddr = 9_000_000

	for i := range numTx {
		sender := smallTxAddr(i)

		var recipient accounts.Address
		if shape.hotPct > 0 && i%100 < shape.hotPct {
			recipient = smallTxAddr(recipientBase + i%hotRecipients)
		} else {
			recipient = smallTxAddr(recipientBase + hotRecipients + i)
		}

		ops := []Op{
			{opType: readType, key: opkey{addr: sender, path: state.NoncePath}, duration: smallTxReadTime, val: 0},
			{opType: writeType, key: opkey{addr: sender, path: state.NoncePath}, duration: smallTxWriteTime, val: 1},
			{opType: readType, key: opkey{addr: sender, path: state.BalancePath}, duration: smallTxReadTime},
			{opType: readType, key: opkey{addr: recipient, path: state.BalancePath}, duration: smallTxReadTime},
			{opType: writeType, key: opkey{addr: sender, path: state.BalancePath}, duration: smallTxWriteTime, val: i},
			{opType: writeType, key: opkey{addr: recipient, path: state.BalancePath}, duration: smallTxWriteTime, val: i},
		}

		if shape.erc20 {
			token := smallTxAddr(tokenAddr)
			fromSlot := smallTxSlot(i)
			toSlot := smallTxSlot(recipientBase + i%hotRecipients)
			ops = append(ops,
				Op{opType: readType, key: opkey{addr: token, key: fromSlot, path: state.StoragePath}, duration: smallTxReadTime},
				Op{opType: readType, key: opkey{addr: token, key: toSlot, path: state.StoragePath}, duration: smallTxReadTime},
				Op{opType: writeType, key: opkey{addr: token, key: fromSlot, path: state.StoragePath}, duration: smallTxWriteTime, val: i},
				Op{opType: writeType, key: opkey{addr: token, key: toSlot, path: state.StoragePath}, duration: smallTxWriteTime, val: i},
			)
		}

		t := NewTestExecTask(i, ops, sender, 0)
		t.setupDelay = smallTxSetup
		t.spin = true
		tasks = append(tasks, t)
	}

	return tasks
}

// runSmallTxBlock times only the parallel execution of one block. It deliberately
// omits runParallel's trailing per-write sleep replay, which at these tx counts
// costs more than the block itself.
func runSmallTxBlock(tb testing.TB, tasks []exec.Task, logger log.Logger) time.Duration {
	tb.Helper()
	ctx := tb.Context()

	dirs := datadir.New(tb.TempDir())
	db := temporaltest.NewTestDB(tb, dirs)

	tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
	assert.NoError(tb, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
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
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, false, logger)),
			logger: logger,
		},
		workerCount: runtime.NumCPU() - 1,
	}

	executorContext, executorCancel, err := pe.run(ctx)
	assert.NoError(tb, err, "error occur during parallel init")
	assert.NoError(tb, executorContext.Err(), "error occur during parallel init")
	defer executorCancel(nil)

	for _, task := range tasks {
		task := task.(*testExecTask)
		task.TxTask.Config = chainSpec.Config
		task.ctx = executorContext //nolint:fatcontext
	}

	start := time.Now()
	_, err = executeParallelWithCheck(tb, pe, tasks, false, nil, false)
	duration := time.Since(start)

	assert.NoError(tb, err, "error occur during parallel execution")

	return duration
}

// BenchmarkSmallTxBlock sweeps block size for blocks made of many cheap txs,
// where per-tx exec-loop cost dominates rather than EVM time.
// Run with: go test -run='^$' -bench=BenchmarkSmallTxBlock -benchtime=1x -count=10
func BenchmarkSmallTxBlock(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)

	shapes := []smallTxShape{
		{name: "transfer", hotPct: 0},
		{name: "transferHot25", hotPct: 25},
		{name: "erc20", erc20: true, hotPct: 0},
		{name: "erc20Hot25", erc20: true, hotPct: 25},
	}
	txCounts := []int{250, 500, 1000, 2000}
	if testing.Short() {
		shapes = shapes[:1]
		txCounts = txCounts[:1]
	}

	for _, shape := range shapes {
		for _, numTx := range txCounts {
			b.Run(fmt.Sprintf("%s/txs=%d", shape.name, numTx), func(b *testing.B) {
				tasks := smallTxTaskFactory(numTx, shape)
				b.ResetTimer()
				d := runSmallTxBlock(b, tasks, logger)
				b.ReportMetric(float64(d.Nanoseconds())/float64(numTx), "ns/tx")
			})
		}
	}
}
