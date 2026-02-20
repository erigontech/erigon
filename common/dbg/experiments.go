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
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
	"unique"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
)

var (
	MaxReorgDepth = EnvUint("MAX_REORG_DEPTH", 96)

	noMemstat            = EnvBool("NO_MEMSTAT", false)
	saveHeapProfile      = EnvBool("SAVE_HEAP_PROFILE", false)
	heapProfileFilePath  = EnvString("HEAP_PROFILE_FILE_PATH", "")
	heapProfileThreshold = EnvUint("HEAP_PROFILE_THRESHOLD", 35)
	heapProfileFrequency = EnvDuration("HEAP_PROFILE_FREQUENCY", 30*time.Second)
	StagesOnlyBlocks     = EnvBool("STAGES_ONLY_BLOCKS", false)

	MdbxLockInRam    = EnvBool("MDBX_LOCK_IN_RAM", false)
	MdbxNoSync       = EnvBool("MDBX_NO_FSYNC", false)
	MdbxNoSyncUnsafe = EnvBool("MDBX_NO_FSYNC_UNSAFE", false)

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

	CaplinSyncedDataMangerDeadlockDetection = EnvBool("CAPLIN_SYNCED_DATA_MANAGER_DEADLOCK_DETECTION", false)

	Exec3Parallel        = EnvBool("EXEC3_PARALLEL", false)
	numWorkers           = runtime.NumCPU() / 2
	Exec3Workers         = EnvInt("EXEC3_WORKERS", numWorkers)
	ExecTerseLoggerLevel = EnvInt("EXEC_TERSE_LOGGER_LEVEL", int(log.LvlWarn))

	CompressWorkers = EnvInt("COMPRESS_WORKERS", 1)
	MergeWorkers    = EnvInt("MERGE_WORKERS", 1)
	CollateWorkers  = EnvInt("COLLATE_WORKERS", 2)

	TraceAccounts        = EnvStrings("TRACE_ACCOUNTS", ",", nil)
	TraceStateKeys       = EnvStrings("TRACE_STATE_KEYS", ",", nil)
	TraceInstructions    = EnvBool("TRACE_INSTRUCTIONS", false)
	TraceTransactionIO   = EnvBool("TRACE_TRANSACTION_IO", false)
	TraceDomainIO        = EnvBool("TRACE_DOMAIN_IO", false)
	TraceNoopIO          = EnvBool("TRACE_NOOP_IO", false)
	TraceLogs            = EnvBool("TRACE_LOGS", false)
	TraceGas             = EnvBool("TRACE_GAS", false)
	TraceDynamicGas      = EnvBool("TRACE_DYNAMIC_GAS", false)
	TraceApply           = EnvBool("TRACE_APPLY", false)
	TraceBlocks          = EnvUints("TRACE_BLOCKS", ",", nil)
	TraceTxIndexes       = EnvInts("TRACE_TXINDEXES", ",", nil)
	TraceUnwinds         = EnvBool("TRACE_UNWINDS", false)
	traceDomains         = EnvStrings("TRACE_DOMAINS", ",", nil)
	StopAfterBlock       = EnvUint("STOP_AFTER_BLOCK", 0)
	BatchCommitments     = EnvBool("BATCH_COMMITMENTS", true)
	CaplinEfficientReorg = EnvBool("CAPLIN_EFFICIENT_REORG", true)
	UseTxDependencies    = EnvBool("USE_TX_DEPENDENCIES", false)
	UseStateCache        = EnvBool("USE_STATE_CACHE", true)

	BorValidateHeaderTime = EnvBool("BOR_VALIDATE_HEADER_TIME", true)
	TraceDeletion         = EnvBool("TRACE_DELETION", false)

	RpcDropResponse = EnvBool("RPC_DROP_RESPONSE", false)
)

func ReadMemStats(m *runtime.MemStats) {
	if noMemstat {
		return
	}
	runtime.ReadMemStats(m)
}

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
		v, _ := envLookup("MDBX_DIRTY_SPACE_MB")
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
		v, _ := envLookup("SLOW_TX")
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
		v, _ := envLookup("LOG_HASH_MISMATCH_REASON")
		if strings.EqualFold(v, "true") {
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

	// above threshold - save heap profile
	var filePath string
	if heapProfileFilePath == "" {
		filePath = filepath.Join(os.TempDir(), "erigon-mem.prof")
	} else {
		filePath = heapProfileFilePath
	}

	if logger != nil {
		logger.Info("[Experiment] saving heap profile as near OOM", "filePath", filePath, "alloc", common.ByteCount(memStats.Alloc))
	}

	// Write heap profile to buffer first
	var buf bytes.Buffer
	if err := pprof.WriteHeapProfile(&buf); err != nil {
		if logger != nil {
			logger.Warn("[Experiment] could not write heap profile to buffer", "err", err)
		}
		return
	}
	logger.Info("[Experiment] wrote heap profile to buffer", "size", common.ByteCount(uint64(buf.Len())))

	// create temp file-> write buffer -> fsync -> rename
	// Writing to the temporary file first and then renaming to the output file ensures durability of data in case of an OOM
	// If there is an OOM crash while writing to the .tmp file we still have the previous heap profile result saved
	// in the output file. The rename() operation is atomic for POSIX systems, so there is no danger of corruption if the OOM comes
	// when os.Rename() is being called.
	tmpPath := filePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		if logger != nil {
			logger.Warn("[Experiment] could not create heap profile temp file", "err", err)
		}
		return
	}
	defer f.Close()
	defer os.Remove(tmpPath) //nolint

	if _, err := f.Write(buf.Bytes()); err != nil {
		if logger != nil {
			logger.Warn("[Experiment] could not write heap profile temp file", "err", err)
		}
		return
	}

	if err := f.Sync(); err != nil {
		if logger != nil {
			logger.Warn("[Experiment] could not sync heap profile temp file", "err", err)
		}
		return
	}

	// Atomic rename (on linux/mac; best-effort on Windows)
	if err := os.Rename(tmpPath, filePath); err != nil {
		if logger != nil {
			logger.Warn("[Experiment] could not rename heap profile file", "err", err)
		}
		return
	}

	logger.Info("[Experiment] wrote heap profile to disk")
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

var tracedBlocks map[uint64]struct{}
var traceAllBlocks bool
var tracedTxIndexes map[int64]struct{}
var tracedAccounts map[unique.Handle[common.Address]]struct{}
var traceAllDomains bool
var tracedDomains map[uint16]struct{}

var traceInit sync.Once

func initTraceMaps() {
	tracedBlocks = map[uint64]struct{}{}
	if len(TraceBlocks) == 1 && TraceBlocks[0] == math.MaxUint64 {
		traceAllBlocks = true
	}
	for _, blockNum := range TraceBlocks {
		tracedBlocks[blockNum] = struct{}{}
	}
	tracedTxIndexes = map[int64]struct{}{}
	for _, index := range TraceTxIndexes {
		tracedTxIndexes[index] = struct{}{}
	}
	tracedAccounts = map[unique.Handle[common.Address]]struct{}{}
	for _, account := range TraceAccounts {
		account, _ = strings.CutPrefix(strings.ToLower(account), "Ox")
		tracedAccounts[unique.Make(common.HexToAddress(account))] = struct{}{}
	}
	if len(traceDomains) == 1 &&
		(strings.EqualFold(traceDomains[0], "all") || strings.EqualFold(traceDomains[0], "any") ||
			strings.EqualFold(traceDomains[0], "true")) {
		traceAllDomains = true
	} else {
		tracedDomains = map[uint16]struct{}{}
		for _, domain := range traceDomains {
			if d, err := string2Domain(domain); err == nil {
				tracedDomains[d] = struct{}{}
			}
		}
	}
}

func string2Domain(in string) (uint16, error) {
	const (
		accountsDomain   uint16 = 0 // Eth Accounts
		storageDomain    uint16 = 1 // Eth Account's Storage
		codeDomain       uint16 = 2 // Eth Smart-Contract Code
		commitmentDomain uint16 = 3 // Merkle Trie
		receiptDomain    uint16 = 4 // Tiny Receipts - without logs. Required for node-operations.
		rCacheDomain     uint16 = 5 // Fat Receipts - with logs. Optional.
		domainLen        uint16 = 6 // Technical marker of Enum. Not real Domain.
	)

	switch strings.ToLower(in) {
	case "accounts":
		return accountsDomain, nil
	case "storage":
		return storageDomain, nil
	case "code":
		return codeDomain, nil
	case "commitment":
		return commitmentDomain, nil
	case "receipt":
		return receiptDomain, nil
	case "rcache":
		return rCacheDomain, nil
	default:
		return math.MaxUint16, fmt.Errorf("unknown name: %s", in)
	}
}

func TraceBlock(blockNum uint64) bool {
	traceInit.Do(initTraceMaps)
	if traceAllBlocks {
		return true
	}
	_, ok := tracedBlocks[blockNum]
	return ok
}

func TraceTx(blockNum uint64, txIndex int) bool {
	traceInit.Do(initTraceMaps)
	if !TraceBlock(blockNum) {
		return false
	}

	if len(tracedTxIndexes) != 0 {
		if _, ok := tracedTxIndexes[int64(txIndex)]; !ok {
			return false
		}
	}

	return true
}

func TraceAccount(addr unique.Handle[common.Address]) bool {
	traceInit.Do(initTraceMaps)
	if len(tracedAccounts) != 0 {
		_, ok := tracedAccounts[addr]
		return ok
	}
	return false
}

func TracingAccounts() bool {
	return len(tracedAccounts) > 0
}

func TraceDomain(domain uint16) bool {
	traceInit.Do(initTraceMaps)
	if traceAllDomains {
		return true
	}
	_, ok := tracedDomains[domain]
	return ok
}
