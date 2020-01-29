package debug

import (
	"os"
	"sync"
)

var gerEnv sync.Once
var ThinHistory bool

var itcEnv sync.Once
var intermediateTrieCache bool

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
