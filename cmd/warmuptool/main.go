// warmuptool drives the commitment Warmuper standalone against an existing
// datadir, replaying a captured set of hashed keys and reporting how long the
// whole warm takes. See execution/commitment/warmup_capture.go for how the key
// file is produced (WARMUP_DUMP_FILE during a real run).
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func main() {
	datadirFlag := flag.String("datadir", "", "erigon datadir (read-only)")
	keysFlag := flag.String("keys", "", "captured keys file: '<hexHashedKey> <startDepth>' per line")
	workersFlag := flag.Int("workers", runtime.NumCPU()*8, "warmup workers")
	limitFlag := flag.Int("limit", 0, "cap number of keys replayed (0 = all)")
	cpuProf := flag.String("cpuprofile", "", "write warm-phase CPU profile here")
	mutexProf := flag.String("mutexprofile", "", "write warm-phase mutex profile here")
	blockProf := flag.String("blockprofile", "", "write warm-phase block profile here")
	flag.Parse()
	if *datadirFlag == "" || *keysFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: warmuptool -datadir <path> -keys <file> [-workers N] [-limit N]")
		os.Exit(2)
	}

	logger := log.New()
	ctx := context.Background()

	keys, depths := mustLoadKeys(*keysFlag, *limitFlag)
	fmt.Printf("loaded %d keys from %s\n", len(keys), *keysFlag)

	dirs := datadir.New(*datadirFlag)
	rawDB := kv2.New(dbcfg.ChainDB, logger).
		Path(dirs.Chaindata).
		RoTxsLimiter(semaphore.NewWeighted(9_000)).
		Readonly(true).
		MustOpen()
	defer rawDB.Close()

	settings, err := dbstate.ResolveErigonDBSettings(dirs, logger, true)
	if err != nil {
		panic(err)
	}
	agg := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).MustOpen(ctx, rawDB)
	defer agg.Close()
	if err := agg.OpenFolder(); err != nil {
		panic(err)
	}
	db, err := temporal.New(rawDB, agg)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	mainTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		panic(err)
	}
	defer mainTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, mainTx, logger)
	if err != nil {
		panic(err)
	}
	defer sd.Close()
	stepSize := sd.StepSize()

	// Per-worker factory: each warmup worker gets its own RoTx + a read-only
	// TrieContext whose Branch() reads the commitment domain (matches the
	// production serial-warmup factory in commitmentdb).
	factory := func() (commitment.PatriciaContext, func()) {
		wtx, err := db.BeginTemporalRo(ctx)
		if err != nil {
			panic(err)
		}
		reader := commitmentdb.NewLatestStateReaderForWorker(ctx, wtx, sd)
		return commitmentdb.NewTrieContextRo(reader, stepSize), func() { wtx.Rollback() }
	}

	w := commitment.NewWarmuper(ctx, commitment.WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: *workersFlag,
		MaxDepth:   commitment.WarmupMaxDepth,
		LogPrefix:  "warmuptool",
	})
	w.Start()

	fmt.Printf("warming %d keys with %d workers...\n", len(keys), *workersFlag)
	if *mutexProf != "" {
		runtime.SetMutexProfileFraction(1)
	}
	if *blockProf != "" {
		runtime.SetBlockProfileRate(1)
	}
	if *cpuProf != "" {
		f, err := os.Create(*cpuProf)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
	}
	start := time.Now()
	for i := range keys {
		w.WarmKey(keys[i], depths[i], 0) // single gen -> WaitBufferFree(0) drains all
	}
	if err := w.WaitBufferFree(0); err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	if *cpuProf != "" {
		pprof.StopCPUProfile()
	}
	writeProfile("mutex", *mutexProf)
	writeProfile("block", *blockProf)
	processed := w.Stats().KeysProcessed
	w.Close()

	fmt.Printf("DONE: processed=%d fed=%d elapsed=%s rate=%.0f keys/s workers=%d\n",
		processed, len(keys), elapsed, float64(processed)/elapsed.Seconds(), *workersFlag)
}

func writeProfile(name, path string) {
	if path == "" {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
		panic(err)
	}
}

func mustLoadKeys(path string, limit int) (keys [][]byte, depths []int) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		line := s.Text()
		sp := strings.IndexByte(line, ' ')
		if sp <= 0 {
			continue
		}
		hk, err := hex.DecodeString(line[:sp])
		if err != nil {
			continue
		}
		d, err := strconv.Atoi(strings.TrimSpace(line[sp+1:]))
		if err != nil {
			continue
		}
		keys = append(keys, hk)
		depths = append(depths, d)
		if limit > 0 && len(keys) >= limit {
			break
		}
	}
	if err := s.Err(); err != nil {
		panic(err)
	}
	return keys, depths
}
