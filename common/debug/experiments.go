package debug

import (
	"os"
	"sync"
)

var gerEnv sync.Once
var ThinHistory bool

var itcEnv sync.Once
var intermediateTrieCache bool

var gndEnv sync.Once
var getNodeData bool

func IsThinHistory() bool {
	gerEnv.Do(func() {
		_, ThinHistory = os.LookupEnv("THIN_HISTORY")
	})
	return ThinHistory
}

func IsIntermediateTrieCache() bool {
	itcEnv.Do(func() {
		_, intermediateTrieCache = os.LookupEnv("INTERMEDIATE_TRIE_CACHE")
	})
	return intermediateTrieCache
}

func IsGetNodeData() bool {
	gndEnv.Do(func() {
		_, getNodeData = os.LookupEnv("GET_NODE_DATA")
	})
	return getNodeData
}
