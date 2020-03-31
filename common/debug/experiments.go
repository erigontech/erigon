package debug

import (
	"os"
	"sync"
	"sync/atomic"
)

var gerEnv sync.Once
var ThinHistory bool

var itcEnv sync.Once

// atomic: bit 0 is the value, bit 1 is the initialized flag
var getNodeData uint32

const (
	gndValueFlag = 1 << iota
	gndInitializedFlag
)

func IsThinHistory() bool {
	gerEnv.Do(func() {
		_, ThinHistory = os.LookupEnv("THIN_HISTORY")
	})
	return ThinHistory
}

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
// according to the presence or absence of DISABLE_GET_NODE_DATA environment variable.
func RestoreGetNodeData() {
	_, envVarSet := os.LookupEnv("DISABLE_GET_NODE_DATA")
	OverrideGetNodeData(!envVarSet)
}

// OverrideGetNodeData allows to explicitly enable or disable the GetNodeData functionality.
func OverrideGetNodeData(val bool) {
	if val {
		atomic.StoreUint32(&getNodeData, gndInitializedFlag|gndValueFlag)
	} else {
		atomic.StoreUint32(&getNodeData, gndInitializedFlag)
	}
}

// --------------- Flags which are enabling additional runtime checks and asserts ----------------
var IntermediateTrieHashAssertDbIntegrity = false
