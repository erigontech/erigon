package debug

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// atomic: bit 0 is the value, bit 1 is the initialized flag
var getNodeData uint32

const (
	gndValueFlag = 1 << iota
	gndInitializedFlag
)

// IsGetNodeData indicates whether the GetNodeData functionality should be enabled.
// By default that's driven by the presence or absence of DISABLE_GET_NODE_DATA environment variable.
func IsGetNodeData() bool {
	x := atomic.LoadUint32(&getNodeData)
	if x&gndInitializedFlag != 0 { // already initialized
		return x&gndValueFlag != 0
	}

	RestoreGetNodeData()
	return IsGetNodeData()
}

// RestoreGetNodeData enables or disables the GetNodeData functionality
// according to the presence or absence of GET_NODE_DATA environment variable.
func RestoreGetNodeData() {
	_, envVarSet := os.LookupEnv("GET_NODE_DATA")
	OverrideGetNodeData(envVarSet)
}

// OverrideGetNodeData allows to explicitly enable or disable the GetNodeData functionality.
func OverrideGetNodeData(val bool) {
	if val {
		atomic.StoreUint32(&getNodeData, gndInitializedFlag|gndValueFlag)
	} else {
		atomic.StoreUint32(&getNodeData, gndInitializedFlag)
	}
}

var (
	testDB    string
	getTestDB sync.Once
)

func TestDB() string {
	getTestDB.Do(func() {
		testDB, _ = os.LookupEnv("TEST_DB")
		if testDB == "" {
			testDB = ""
		}
	})
	return testDB
}

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
