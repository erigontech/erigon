package pool

import (
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
)

var operationsMultiplier = 20 // Cap the amount of cached element to max_operations_per_block * operations_multiplier

type OperationPool[K comparable, T any] struct {
	pool *lru.Cache[K, T] // Map the Signature to the underlying object
}

func NewOperationPool[K comparable, T any](maxOperationsPerBlock int, matricName string) *OperationPool[K, T] {
	pool, err := lru.New[K, T](matricName, maxOperationsPerBlock*operationsMultiplier)
	if err != nil {
		panic(err)
	}
	return &OperationPool[K, T]{pool: pool}
}

func (o *OperationPool[K, T]) Insert(k K, operation T) {
	o.pool.Add(k, operation)
}

func (o *OperationPool[K, T]) DeleteIfExist(k K) (removed bool) {
	return o.pool.Remove(k)
}

func (o *OperationPool[K, T]) Has(k K) (hash bool) {
	return o.pool.Contains(k)
}

func (o *OperationPool[K, T]) Raw() []T {
	return o.pool.Values()
}
