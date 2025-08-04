package wit

import (
	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon-lib/common"
)

const (
	// MaxKnownWitnesses is the maximum number of witness hashes to keep in the known list
	MaxKnownWitnesses = 1000

	// MaxQueuedWitnesses is the maximum number of witness propagations to queue up before
	// dropping broadcasts
	MaxQueuedWitnesses = 10

	// MaxQueuedWitnessAnns is the maximum number of witness announcements to queue up before
	// dropping broadcasts
	MaxQueuedWitnessAnns = 10
)

// KnownCache is a cache for known witness, identified by the hash of the parent witness block.
type KnownCache struct {
	hashes mapset.Set[common.Hash]
	max    int
}

// NewKnownCache creates a new knownCache with a max capacity.
func NewKnownCache(max int) *KnownCache {
	return &KnownCache{
		max:    max,
		hashes: mapset.NewSet[common.Hash](),
	}
}

// Add adds a witness to the set.
func (k *KnownCache) Add(hash common.Hash) {
	for k.hashes.Cardinality() > max(0, k.max-1) {
		k.hashes.Pop()
	}
	k.hashes.Add(hash)
}

// Contains returns whether the given item is in the set.
func (k *KnownCache) Contains(hash common.Hash) bool {
	return k.hashes.Contains(hash)
}

// Cardinality returns the number of elements in the set.
func (k *KnownCache) Cardinality() int {
	return k.hashes.Cardinality()
}
