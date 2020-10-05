package debug

import (
	"os"
	"sync"
	"sync/atomic"
)

var (
	compressBlocks      bool
	getCompressBlocks   sync.Once
	compressReceipts    bool
	getCompressReceipts sync.Once
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

func IsBlockCompressionEnabled() bool {
	getCompressBlocks.Do(func() {
		_, compressBlocks = os.LookupEnv("COMPRESS_BLOCKS")
	})
	return compressBlocks
}

func IsReceiptsCompressionEnabled() bool {
	getCompressReceipts.Do(func() {
		_, compressReceipts = os.LookupEnv("COMPRESS_RECEIPTS")
	})
	return compressReceipts
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
