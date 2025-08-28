// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package dbg

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
)

var (
	MaxReorgDepth = EnvInt("MAX_REORG_DEPTH", 512)

	noMemstat            = EnvBool("NO_MEMSTAT", false)
	saveHeapProfile      = EnvBool("SAVE_HEAP_PROFILE", false)
	heapProfileFilePath  = EnvString("HEAP_PROFILE_FILE_PATH", "")
	heapProfileThreshold = EnvUint("HEAP_PROFILE_THRESHOLD", 35)
	heapProfileFrequency = EnvDuration("HEAP_PROFILE_FREQUENCY", 30*time.Second)
	mdbxLockInRam        = EnvBool("MDBX_LOCK_IN_RAM", false)
	StagesOnlyBlocks     = EnvBool("STAGES_ONLY_BLOCKS", false)

	stopBeforeStage = EnvString("STOP_BEFORE_STAGE", "")
	stopAfterStage  = EnvString("STOP_AFTER_STAGE", "")

	mergeTr = EnvInt("MERGE_THRESHOLD", -1)

	//state v3
	noPrune              = EnvBool("NO_PRUNE", false)
	noMerge              = EnvBool("NO_MERGE", false)
	discardCommitment    = EnvBool("DISCARD_COMMITMENT", false)
	pruneTotalDifficulty = EnvBool("PRUNE_TOTAL_DIFFICULTY", true)

	// force skipping of any non-Erigon2 .torrent files
	DownloaderOnlyBlocks = EnvBool("DOWNLOADER_ONLY_BLOCKS", false)

	// allows to collect reading metrics for kv by file level
	KVReadLevelledMetrics = EnvBool("KV_READ_METRICS", false)

	// allow simultaneous build of multiple snapshot types.
	// Values from 1 to 4 makes sense since we have only 3 types of snapshots.
	BuildSnapshotAllowance = EnvInt("SNAPSHOT_BUILD_SEMA_SIZE", 1) // allows 1 kind of snapshots to be built simultaneously

	SnapshotMadvRnd = EnvBool("SNAPSHOT_MADV_RND", true)
	OnlyCreateDB    = EnvBool("ONLY_CREATE_DB", false)

	CommitEachStage = EnvBool("COMMIT_EACH_STAGE", false)

	CaplinSyncedDataMangerDeadlockDetection = EnvBool("CAPLIN_SYNCED_DATA_MANAGER_DEADLOCK_DETECTION", false)

	Exec3Parallel = EnvBool("EXEC3_PARALLEL", false)
	numWorkers    = runtime.NumCPU() / 2
	Exec3Workers  = EnvInt("EXEC3_WORKERS", numWorkers)

	TraceAccounts        = EnvStrings("TRACE_ACCOUNTS", ",", nil)
	TraceStateKeys       = EnvStrings("TRACE_STATE_KEYS", ",", nil)
	TraceInstructions    = EnvBool("TRACE_INSTRUCTIONS", false)
	TraceTransactionIO   = EnvBool("TRACE_TRANSACTION_IO", false)
	TraceBlocks          = EnvUints("TRACE_BLOCKS", ",", nil)
	TraceTxIndexes       = EnvInts("TRACE_TRANSACTIONS", ",", nil)
	StopAfterBlock       = EnvUint("STOP_AFTER_BLOCK", 0)
	BatchCommitments     = EnvBool("BATCH_COMMITMENTS", true)
	CaplinEfficientReorg = EnvBool("CAPLIN_EFFICIENT_REORG", true)
	UseTxDependencies    = EnvBool("USE_TX_DEPENDENCIES", false)
	TraceDeletion        = EnvBool("TRACE_DELETION", false)
)

func ReadMemStats(m *runtime.MemStats) {
	if noMemstat {
		return
	}
	runtime.ReadMemStats(m)
}

func MdbxLockInRam() bool { return mdbxLockInRam }

func DiscardCommitment() bool    { return discardCommitment }
func NoPrune() bool              { return noPrune }
func NoMerge() bool              { return noMerge }
func PruneTotalDifficulty() bool { return pruneTotalDifficulty }

var (
	dirtySace     uint64
	dirtySaceOnce sync.Once
)

func DirtySpace() uint64 {
	dirtySaceOnce.Do(func() {
		v, _ := os.LookupEnv("MDBX_DIRTY_SPACE_MB")
		if v != "" {
			i := MustParseInt(v)
			log.Info("[Experiment]", "MDBX_DIRTY_SPACE_MB", i)
			dirtySace = uint64(i * 1024 * 1024)
		}
	})
	return dirtySace
}

func MergeTr() int { return mergeTr }

var (
	slowTx     time.Duration
	slowTxOnce sync.Once
)

func SlowTx() time.Duration {
	slowTxOnce.Do(func() {
		v, _ := os.LookupEnv("SLOW_TX")
		if v != "" {
			var err error
			slowTx, err = time.ParseDuration(v)
			if err != nil {
				panic(err)
			}
			log.Info("[Experiment]", "SLOW_TX", slowTx.String())
		}
	})
	return slowTx
}

func StopBeforeStage() string { return stopBeforeStage }

// TODO(allada) We should possibly consider removing `STOP_BEFORE_STAGE`, as `STOP_AFTER_STAGE` can
// perform all same the functionality, but due to reverse compatibility reasons we are going to
// leave it.
func StopAfterStage() string { return stopAfterStage }

var (
	logHashMismatchReason     bool
	logHashMismatchReasonOnce sync.Once
)

func LogHashMismatchReason() bool {
	logHashMismatchReasonOnce.Do(func() {
		v, _ := os.LookupEnv("LOG_HASH_MISMATCH_REASON")
		if v == "true" {
			logHashMismatchReason = true
			log.Info("[Experiment]", "LOG_HASH_MISMATCH_REASON", logHashMismatchReason)
		}
	})
	return logHashMismatchReason
}

type saveHeapOptions struct {
	memStats *runtime.MemStats
	logger   *log.Logger
}

type SaveHeapOption func(options *saveHeapOptions)

func SaveHeapWithMemStats(memStats *runtime.MemStats) SaveHeapOption {
	return func(options *saveHeapOptions) {
		options.memStats = memStats
	}
}

func SaveHeapWithLogger(logger *log.Logger) SaveHeapOption {
	return func(options *saveHeapOptions) {
		options.logger = logger
	}
}

func SaveHeapProfileNearOOM(opts ...SaveHeapOption) {
	if !saveHeapProfile {
		return
	}

	var options saveHeapOptions
	for _, opt := range opts {
		opt(&options)
	}

	var logger log.Logger
	if options.logger != nil {
		logger = *options.logger
	}

	var memStats runtime.MemStats
	if options.memStats != nil {
		memStats = *options.memStats
	} else {
		ReadMemStats(&memStats)
	}

	totalMemory := estimate.TotalMemory()
	if logger != nil {
		logger.Info(
			"[Experiment] heap profile threshold check",
			"alloc", common.ByteCount(memStats.Alloc),
			"total", common.ByteCount(totalMemory),
		)
	}
	if memStats.Alloc < (totalMemory/100)*heapProfileThreshold {
		return
	}

	// above 45%
	var filePath string
	if heapProfileFilePath == "" {
		filePath = filepath.Join(os.TempDir(), "erigon-mem.prof")
	} else {
		filePath = heapProfileFilePath
	}
	if logger != nil {
		logger.Info("[Experiment] saving heap profile as near OOM", "filePath", filePath)
	}

	f, err := os.Create(filePath)
	if err != nil && logger != nil {
		logger.Warn("[Experiment] could not create heap profile file", "err", err)
	}

	defer func() {
		err := f.Close()
		if err != nil && logger != nil {
			logger.Warn("[Experiment] could not close heap profile file", "err", err)
		}
	}()

	runtime.GC()
	err = pprof.WriteHeapProfile(f)
	if err != nil && logger != nil {
		logger.Warn("[Experiment] could not write heap profile file", "err", err)
	}
}

func SaveHeapProfileNearOOMPeriodically(ctx context.Context, opts ...SaveHeapOption) {
	if !saveHeapProfile {
		return
	}

	ticker := time.NewTicker(heapProfileFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			SaveHeapProfileNearOOM(opts...)
		}
	}
}
