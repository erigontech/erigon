package debug

import (
	"os"
	"sync"
	"sync/atomic"
)

var gerEnv sync.Once
var ThinHistory bool

var itcEnv sync.Once
var intermediateTrieHash bool

var getNodeData uint32 // atomic: bit 0 is the value, bit 1 is the initialized flag

func IsThinHistory() bool {
	gerEnv.Do(func() {
		_, ThinHistory = os.LookupEnv("THIN_HISTORY")
	})
	return ThinHistory
}

func IsIntermediateTrieHash() bool {
	itcEnv.Do(func() {
		_, intermediateTrieHash = os.LookupEnv("INTERMEDIATE_TRIE_CACHE")
		if !intermediateTrieHash {
			_, intermediateTrieHash = os.LookupEnv("INTERMEDIATE_TRIE_HASH")
		}
	})
	return intermediateTrieHash
}

// IsGetNodeData indicates whether the GetNodeData functionality should be enabled.
// By default that's driven by the presence or absence of GET_NODE_DATA environment variable.
func IsGetNodeData() bool {
	x := atomic.LoadUint32(&getNodeData)
	if x >= 2 { // already initialized
		return x%2 != 0
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
		atomic.StoreUint32(&getNodeData, 3)
	} else {
		atomic.StoreUint32(&getNodeData, 2)
	}
}
