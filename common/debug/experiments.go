package debug

import (
	"os"
	"strconv"
	"sync"
	"time"
)

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
		}
	})
	return bigRwTx
}

var (
	slowCommit    time.Duration
	getSlowCommit sync.Once
)

func SlowCommit() time.Duration {
	getSlowCommit.Do(func() {
		v, _ := os.LookupEnv("DEBUG_SLOW_COMMIT_MS")
		if v != "" {
			i, err := strconv.Atoi(v)
			if err != nil {
				panic(err)
			}
			slowCommit = time.Duration(i) * time.Millisecond
		}
	})
	return slowCommit
}

var (
	stopBeforeStage     string
	stopBeforeStageFlag sync.Once
	stopAfterStage      string
	stopAfterStageFlag  sync.Once
)

func StopBeforeStage() string {
	f := func() {
		v, _ := os.LookupEnv("STOP_BEFORE_STAGE") // see names in eth/stagedsync/stages/stages.go
		if v != "" {
			stopBeforeStage = v
		}
	}
	stopBeforeStageFlag.Do(f)
	return stopBeforeStage
}

// TODO(allada) We should possibly consider removing `STOP_BEFORE_STAGE`, as `STOP_AFTER_STAGE` can
// perform all same the functionality, but due to reverse compatibility reasons we are going to
// leave it.
func StopAfterStage() string {
	f := func() {
		v, _ := os.LookupEnv("STOP_AFTER_STAGE") // see names in eth/stagedsync/stages/stages.go
		if v != "" {
			stopAfterStage = v
		}
	}
	stopAfterStageFlag.Do(f)
	return stopAfterStage
}
