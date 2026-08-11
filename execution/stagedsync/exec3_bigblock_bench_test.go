package stagedsync

import (
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"

	"github.com/erigontech/erigon/common"
	"github.com/holiman/uint256"
)

// BenchmarkMoreConflictsBig is BenchmarkMoreConflicts at one point of the sweep,
// with the block size raised well past the checked-in 300.
// Run with: BIGBLOCK_TXS=10000 go test -run='^$' -bench=BenchmarkMoreConflictsBig$ -benchtime=10x ./execution/stagedsync/
func BenchmarkMoreConflictsBig(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	numRead, numWrite, numNonIO := 20, 20, 100

	for _, numTx := range bigBlockTxCounts(b, []int{10_000}) {
		b.Run(fmt.Sprintf("txs=%d", numTx), func(b *testing.B) {
			var total, serial time.Duration
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				rng := rand.New(rand.NewSource(int64(i)))
				tasks, serialDuration := taskFactory(numTx, moreConflictsSender(rng), numRead, numWrite, numNonIO, randomPathGenerator, readTime, writeTime, nonIOTime)
				b.StartTimer()
				total += runParallel(b, tasks, defaultChecks, false, logger)
				serial += serialDuration
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(numTx), "ns/tx")
			b.ReportMetric(float64(serial)/float64(total), "speedup")
		})
	}
}

// BenchmarkMoreConflictsBigSerialFloor drops all simulated per-op cost, so what
// is left is the executor's own work — scheduling, versionMap, apply loop. It
// also reports executions and aborts per tx, which say whether the block shape
// leaves the workers any room to be the limit.
func BenchmarkMoreConflictsBigSerialFloor(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	zero := func(txIdx int, opIdx int) time.Duration { return 0 }

	for _, numTx := range bigBlockTxCounts(b, []int{300, 1000, 2000, 5000, 10_000}) {
		b.Run(fmt.Sprintf("txs=%d", numTx), func(b *testing.B) {
			var total time.Duration
			var execs, aborts int64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				rng := rand.New(rand.NewSource(int64(i)))
				tasks, _ := taskFactory(numTx, moreConflictsSender(rng), 20, 20, 100, randomPathGenerator, zero, zero, zero)
				for _, t := range tasks {
					tt := t.(*testExecTask)
					tt.setupDelay = 0
					for k := range tt.ops {
						if tt.ops[k].opType == writeType {
							tt.ops[k].val = k + 1
						}
					}
				}
				d, e, a := runParallelStats(b, tasks, logger)
				total += d
				execs += e
				aborts += a
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(numTx), "ns/tx")
			b.ReportMetric(float64(execs)/float64(b.N)/float64(numTx), "execs/tx")
			b.ReportMetric(float64(aborts)/float64(b.N)/float64(numTx), "aborts/tx")
		})
	}
}

// runParallelStats is runParallel without the trailing write-set replay,
// reporting how many task executions the block cost and how many were aborted.
// It runs the timer itself: building the db and seeding the state costs more the
// wider the write sets get, which is the axis these benchmarks sweep, so leaving
// it in ns/op would dilute the very effect they measure. The caller stops the
// timer before it builds the tasks.
func runParallelStats(b *testing.B, tasks []exec.Task, logger log.Logger) (time.Duration, int64, int64) {
	b.Helper()
	ctx := b.Context()

	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDB(b, dirs)

	tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
	require.NoError(b, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(b, err)
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
		workerCount: bigBlockWorkers(),
	}

	executorContext, executorCancel, err := pe.run(ctx)
	require.NoError(b, err)
	defer executorCancel(nil)

	for _, task := range tasks {
		task := task.(*testExecTask)
		task.TxTask.Config = chainSpec.Config
		task.ctx = executorContext //nolint:fatcontext
	}

	seedTaskState(b, domains, tx, tasks)

	b.StartTimer()
	start := time.Now()
	_, err = executeParallelWithCheck(b, pe, tasks, false, nil, false)
	d := time.Since(start)
	b.StopTimer()
	require.NoError(b, err)

	return d, pe.execCount.Load(), pe.abortCount.Load()
}

// seedTaskState makes every account the tasks touch exist, and pre-fills every
// slot they write with a value they will overwrite. Without it Normalize drops
// the whole write set as no-ops on absent accounts, so the apply loop never
// reaches ApplyStateWrites and the block measures nothing but scheduling.
func seedTaskState(tb testing.TB, domains *execctx.SharedDomains, tx kv.TemporalTx, tasks []exec.Task) {
	tb.Helper()

	seeded := map[accounts.Address]struct{}{}
	seedAccount := func(addr accounts.Address) {
		if _, ok := seeded[addr]; ok {
			return
		}
		seeded[addr] = struct{}{}
		av := addr.Value()
		acc := accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1_000_000)}
		require.NoError(tb, domains.DomainPut(kv.AccountsDomain, tx, av[:], accounts.SerialiseV3(&acc), 0, nil))
	}

	var slotKey []byte
	for _, task := range tasks {
		t := task.(*testExecTask)
		seedAccount(t.sender)
		for _, op := range t.ops {
			if op.opType != writeType || op.key.path != state.StoragePath {
				continue
			}
			seedAccount(op.key.addr)
			av := op.key.addr.Value()
			kv2 := op.key.key.Value()
			slotKey = append(slotKey[:0], av[:]...)
			slotKey = append(slotKey, kv2[:]...)
			prev := uint256.NewInt(uint64(op.val) + 1)
			require.NoError(tb, domains.DomainPut(kv.StorageDomain, tx, slotKey, prev.Bytes(), 0, nil))
		}
	}
}

func bigBlockTxCounts(b *testing.B, dflt []int) []int {
	v := os.Getenv("BIGBLOCK_TXS")
	if v == "" {
		return dflt
	}
	var out []int
	for _, s := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			b.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

func bigBlockWorkers() int {
	if v := os.Getenv("BIGBLOCK_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU() - 1
}

// widePathGenerator gives every op its own slot, so a tx's read and write sets
// are as large as its op counts. randomPathGenerator ignores the op index and
// collapses a whole tx onto one slot, which leaves the apply loop two entries
// per tx and nothing to show on a profile.
func widePathGenerator(contracts int) PathGenerator {
	return func(i int, j int, total int) opkey {
		addr := accounts.InternAddress(common.BigToAddress(big.NewInt(int64(j % contracts))))
		key := accounts.InternKey(common.BigToHash(big.NewInt(int64(i*total + j))))
		return opkey{addr, key, state.StoragePath}
	}
}

// BenchmarkApplyLoop sweeps the per-tx write-set size at a fixed block size,
// with no simulated op cost, so the serial apply loop is what grows.
func BenchmarkApplyLoop(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip()
	}
	logger := logger(discardLogging)
	zero := func(txIdx int, opIdx int) time.Duration { return 0 }
	gen := widePathGenerator(8)

	numTx := bigBlockTxCounts(b, []int{10_000})[0]

	for _, perTx := range []int{2, 10, 25, 50, 100} {
		b.Run(fmt.Sprintf("txs=%d/writes=%d", numTx, perTx), func(b *testing.B) {
			var total time.Duration
			var execs, aborts int64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				rng := rand.New(rand.NewSource(int64(i)))
				tasks, _ := taskFactory(numTx, moreConflictsSender(rng), perTx, perTx, 0, gen, zero, zero, zero)
				for _, t := range tasks {
					t.(*testExecTask).setupDelay = 0
				}
				d, e, a := runParallelStats(b, tasks, logger)
				total += d
				execs += e
				aborts += a
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(numTx), "ns/tx")
			b.ReportMetric(float64(execs)/float64(b.N)/float64(numTx), "execs/tx")
			b.ReportMetric(float64(aborts)/float64(b.N)/float64(numTx), "aborts/tx")
		})
	}
}
