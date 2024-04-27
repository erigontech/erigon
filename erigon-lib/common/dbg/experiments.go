/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package dbg

import (
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"
)

var (
	doMemstat        = EnvBool("NO_MEMSTAT", true)
	writeMap         = EnvBool("WRITE_MAP", false)
	noSync           = EnvBool("NO_SYNC", false)
	mdbxReadahead    = EnvBool("MDBX_READAHEAD", false)
	mdbxLockInRam    = EnvBool("MDBX_LOCK_IN_RAM", false)
	StagesOnlyBlocks = EnvBool("STAGES_ONLY_BLOCKS", false)

	stopBeforeStage = EnvString("STOP_BEFORE_STAGE", "")
	stopAfterStage  = EnvString("STOP_AFTER_STAGE", "")

	mergeTr = EnvInt("MERGE_THRESHOLD", -1)

	//state v3
	noPrune           = EnvBool("NO_PRUNE", false)
	noMerge           = EnvBool("NO_MERGE", false)
	discardHistory    = EnvBool("DISCARD_HISTORY", false)
	discardCommitment = EnvBool("DISCARD_COMMITMENT", false)

	// force skipping of any non-Erigon2 .torrent files
	DownloaderOnlyBlocks = EnvBool("DOWNLOADER_ONLY_BLOCKS", false)

	// run prune on flush with given timeout. If timeout is 0, no prune on flush will be performed
	PruneOnFlushTimeout = EnvDuration("PRUNE_ON_FLUSH_TIMEOUT", time.Duration(0))

	// allow simultaneous build of multiple snapshot types.
	// Values from 1 to 4 makes sense since we have only 3 types of snapshots.

	BuildSnapshotAllowance = EnvInt("SNAPSHOT_BUILD_SEMA_SIZE", 1)

	SnapshotMadvRnd = EnvBool("SNAPSHOT_MADV_RND", true)
	OnlyCreateDB    = EnvBool("ONLY_CREATE_DB", false)
)

func ReadMemStats(m *runtime.MemStats) {
	if doMemstat {
		runtime.ReadMemStats(m)
	}
}

func WriteMap() bool      { return writeMap }
func NoSync() bool        { return noSync }
func MdbxReadAhead() bool { return mdbxReadahead }
func MdbxLockInRam() bool { return mdbxLockInRam }

func DiscardHistory() bool    { return discardHistory }
func DiscardCommitment() bool { return discardCommitment }
func NoPrune() bool           { return noPrune }
func NoMerge() bool           { return noMerge }

var (
	dirtySace     uint64
	dirtySaceOnce sync.Once
)

func DirtySpace() uint64 {
	dirtySaceOnce.Do(func() {
		v, _ := os.LookupEnv("MDBX_DIRTY_SPACE_MB")
		if v != "" {
			i, err := strconv.Atoi(v)
			if err != nil {
				panic(err)
			}
			log.Info("[Experiment]", "MDBX_DIRTY_SPACE_MB", i)
			dirtySace = uint64(i * 1024 * 1024)
		}
	})
	return dirtySace
}

func MergeTr() int { return mergeTr }

var (
	bigRoTx    uint
	getBigRoTx sync.Once
)

// DEBUG_BIG_RO_TX_KB - print logs with info about large read-only transactions
// DEBUG_BIG_RW_TX_KB - print logs with info about large read-write transactions
// DEBUG_SLOW_COMMIT_MS - print logs with commit timing details if commit is slower than this threshold
func BigRoTxKb() uint {
	getBigRoTx.Do(func() {
		v, _ := os.LookupEnv("DEBUG_BIG_RO_TX_KB")
		if v != "" {
			i, err := strconv.Atoi(v)
			if err != nil {
				panic(err)
			}
			bigRoTx = uint(i)
			log.Info("[Experiment]", "DEBUG_BIG_RO_TX_KB", bigRoTx)
		}
	})
	return bigRoTx
}

var (
	bigRwTx    uint
	getBigRwTx sync.Once
)

func BigRwTxKb() uint {
	getBigRwTx.Do(func() {
		v, _ := os.LookupEnv("DEBUG_BIG_RW_TX_KB")
		if v != "" {
			i, err := strconv.Atoi(v)
			if err != nil {
				panic(err)
			}
			bigRwTx = uint(i)
			log.Info("[Experiment]", "DEBUG_BIG_RW_TX_KB", bigRwTx)
		}
	})
	return bigRwTx
}

var (
	slowCommit     time.Duration
	slowCommitOnce sync.Once
)

func SlowCommit() time.Duration {
	slowCommitOnce.Do(func() {
		v, _ := os.LookupEnv("SLOW_COMMIT")
		if v != "" {
			var err error
			slowCommit, err = time.ParseDuration(v)
			if err != nil {
				panic(err)
			}
			log.Info("[Experiment]", "SLOW_COMMIT", slowCommit.String())
		}
	})
	return slowCommit
}

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
	stopAfterReconst     bool
	stopAfterReconstOnce sync.Once
)

func StopAfterReconst() bool {
	stopAfterReconstOnce.Do(func() {
		v, _ := os.LookupEnv("STOP_AFTER_RECONSTITUTE")
		if v == "true" {
			stopAfterReconst = true
			log.Info("[Experiment]", "STOP_AFTER_RECONSTITUTE", stopAfterReconst)
		}
	})
	return stopAfterReconst
}

var (
	snapshotVersion     uint8
	snapshotVersionOnce sync.Once
)

func SnapshotVersion() uint8 {
	snapshotVersionOnce.Do(func() {
		v, _ := os.LookupEnv("SNAPSHOT_VERSION")
		if i, _ := strconv.ParseUint(v, 10, 8); i > 0 {
			snapshotVersion = uint8(i)
			log.Info("[Experiment]", "SNAPSHOT_VERSION", snapshotVersion)
		}
	})
	return snapshotVersion
}

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
