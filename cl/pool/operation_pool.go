package pool

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
)

var operationsMultiplier = 10 // Cap the amount of cached element to max_operations_per_block * operations_multiplier

type OperationPool[T any] struct {
	pool *lru.Cache[libcommon.Bytes96, T] // Map the Signature to the underlying object
}

func NewOperationPool[T any](maxOperationsPerBlock int, matricName string) *OperationPool[T] {
	pool, err := lru.New[libcommon.Bytes96, T](matricName, maxOperationsPerBlock*operationsMultiplier)
	if err != nil {
		panic(err)
	}
	return &OperationPool[T]{pool: pool}
}

func (o *OperationPool[T]) Insert(signature libcommon.Bytes96, operation T) {
	o.pool.Add(signature, operation)
}

func (o *OperationPool[T]) DeleteIfExist(signature libcommon.Bytes96) (removed bool) {
	return o.pool.Remove(signature)
}

func (o *OperationPool[T]) Raw() []T {
	return o.pool.Values()
}
